/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.checker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.VolumeCheckContext;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;

/**
 * A class that encapsulates running disk checks against each volume of an
 * {@link FsDatasetSpi} and allows retrieving a list of failed volumes.
 *
 * This splits out behavior that was originally implemented across
 * DataNode, FsDatasetImpl and FsVolumeList.
 */
public class DatasetVolumeChecker {

  public static final Logger LOG =
      LoggerFactory.getLogger(DatasetVolumeChecker.class);

  private AsyncChecker<VolumeCheckContext, VolumeCheckResult> delegateChecker;

  private final AtomicLong numVolumeChecks = new AtomicLong(0);
  private final AtomicLong numSyncDatasetChecks = new AtomicLong(0);
  private final AtomicLong numAsyncDatasetChecks = new AtomicLong(0);
  private final AtomicLong numSkippedChecks = new AtomicLong(0);

  /**
   * Max allowed time for a disk check in milliseconds. If the check
   * doesn't complete within this time we declare the disk as dead.
   */
  private final long maxAllowedTimeForCheckMs;

  /**
   * Maximum number of volume failures that can be tolerated without
   * declaring a fatal error.
   */
  private final int maxVolumeFailuresTolerated;

  /**
   * Minimum time between two successive disk checks of a volume.
   */
  private final long minDiskCheckGapMs;

  /**
   * Timestamp of the last check of all volumes.
   */
  private long lastAllVolumesCheck;

  private final Timer timer;

  private static final VolumeCheckContext IGNORED_CONTEXT =
      new VolumeCheckContext();

  /**
   * @param conf Configuration object.
   * @param timer {@link Timer} object used for throttling checks.
   */
  public DatasetVolumeChecker(Configuration conf, Timer timer)
      throws DiskErrorException {
    maxAllowedTimeForCheckMs = conf.getTimeDuration(
        DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    if (maxAllowedTimeForCheckMs <= 0) {
      throw new DiskErrorException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY + " - "
          + maxAllowedTimeForCheckMs + " (should be > 0)");
    }

    this.timer = timer;

    maxVolumeFailuresTolerated = conf.getInt(
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    minDiskCheckGapMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_DEFAULT,
        TimeUnit.MILLISECONDS);

    if (minDiskCheckGapMs < 0) {
      throw new DiskErrorException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY + " - "
          + minDiskCheckGapMs + " (should be >= 0)");
    }

    lastAllVolumesCheck = timer.monotonicNow() - minDiskCheckGapMs;

    if (maxVolumeFailuresTolerated < 0) {
      throw new DiskErrorException("Invalid value configured for "
          + DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY + " - "
          + maxVolumeFailuresTolerated + " (should be non-negative)");
    }

    delegateChecker = new ThrottledAsyncChecker<>(
        timer, minDiskCheckGapMs, Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("DataNode DiskChecker thread %d")
                .setDaemon(true)
                .build()));
  }

  /**
   * Run checks against all volumes of a dataset.
   *
   * This check may be performed at service startup and subsequently at
   * regular intervals to detect and handle failed volumes.
   *
   * @param dataset - FsDatasetSpi to be checked.
   * @return set of failed volumes.
   */
  public Set<FsVolumeSpi> checkAllVolumes(
      final FsDatasetSpi<? extends FsVolumeSpi> dataset)
      throws InterruptedException {
    final long gap = timer.monotonicNow() - lastAllVolumesCheck;
    if (gap < minDiskCheckGapMs) {
      numSkippedChecks.incrementAndGet();
      LOG.trace(
          "Skipped checking all volumes, time since last check {} is less " +
          "than the minimum gap between checks ({} ms).",
          gap, minDiskCheckGapMs);
      return Collections.emptySet();
    }

    final FsDatasetSpi.FsVolumeReferences references =
        dataset.getFsVolumeReferences();

    if (references.size() == 0) {
      LOG.warn("checkAllVolumesAsync - no volumes can be referenced");
      return Collections.emptySet();
    }

    lastAllVolumesCheck = timer.monotonicNow();
    final Set<FsVolumeSpi> healthyVolumes = new HashSet<>();
    final Set<FsVolumeSpi> failedVolumes = new HashSet<>();
    final Set<FsVolumeSpi> allVolumes = new HashSet<>();

    final AtomicLong numVolumes = new AtomicLong(references.size());
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < references.size(); ++i) {
      final FsVolumeReference reference = references.getReference(i);
      allVolumes.add(reference.getVolume());
      ListenableFuture<VolumeCheckResult> future =
          delegateChecker.schedule(reference.getVolume(), IGNORED_CONTEXT);
      LOG.info("Scheduled health check for volume {}", reference.getVolume());
      Futures.addCallback(future, new ResultHandler(
          reference, healthyVolumes, failedVolumes, numVolumes, new Callback() {
        @Override
        public void call(Set<FsVolumeSpi> ignored1,
                         Set<FsVolumeSpi> ignored2) {
          latch.countDown();
        }
      }));
    }

    // Wait until our timeout elapses, after which we give up on
    // the remaining volumes.
    if (!latch.await(maxAllowedTimeForCheckMs, TimeUnit.MILLISECONDS)) {
      LOG.warn("checkAllVolumes timed out after {} ms" +
          maxAllowedTimeForCheckMs);
    }

    numSyncDatasetChecks.incrementAndGet();
    synchronized (this) {
      // All volumes that have not been detected as healthy should be
      // considered failed. This is a superset of 'failedVolumes'.
      //
      // Make a copy under the mutex as Sets.difference() returns a view
      // of a potentially changing set.
      return new HashSet<>(Sets.difference(allVolumes, healthyVolumes));
    }
  }

  /**
   * Start checks against all volumes of a dataset, invoking the
   * given callback when the operation has completed. The function
   * does not wait for the checks to complete.
   *
   * If a volume cannot be referenced then it is already closed and
   * cannot be checked. No error is propagated to the callback for that
   * volume.
   *
   * @param dataset - FsDatasetSpi to be checked.
   * @param callback - Callback to be invoked when the checks are complete.
   * @return true if the check was scheduled and the callback will be invoked.
   *         false if the check was not scheduled and the callback will not be
   *         invoked.
   */
  public boolean checkAllVolumesAsync(
      final FsDatasetSpi<? extends FsVolumeSpi> dataset,
      Callback callback) {
    final long gap = timer.monotonicNow() - lastAllVolumesCheck;
    if (gap < minDiskCheckGapMs) {
      numSkippedChecks.incrementAndGet();
      LOG.trace(
          "Skipped checking all volumes, time since last check {} is less " +
              "than the minimum gap between checks ({} ms).",
          gap, minDiskCheckGapMs);
      return false;
    }

    final FsDatasetSpi.FsVolumeReferences references =
        dataset.getFsVolumeReferences();

    if (references.size() == 0) {
      LOG.warn("checkAllVolumesAsync - no volumes can be referenced");
      return false;
    }

    lastAllVolumesCheck = timer.monotonicNow();
    final Set<FsVolumeSpi> healthyVolumes = new HashSet<>();
    final Set<FsVolumeSpi> failedVolumes = new HashSet<>();
    final AtomicLong numVolumes = new AtomicLong(references.size());

    LOG.info("Checking {} volumes", references.size());
    for (int i = 0; i < references.size(); ++i) {
      final FsVolumeReference reference = references.getReference(i);
      // The context parameter is currently ignored.
      ListenableFuture<VolumeCheckResult> future =
          delegateChecker.schedule(reference.getVolume(), IGNORED_CONTEXT);
      Futures.addCallback(future, new ResultHandler(
          reference, healthyVolumes, failedVolumes, numVolumes, callback));
    }
    numAsyncDatasetChecks.incrementAndGet();
    return true;
  }

  /**
   * A callback interface that is supplied the result of running an
   * async disk check on multiple volumes.
   */
  public interface Callback {
    /**
     * @param healthyVolumes set of volumes that passed disk checks.
     * @param failedVolumes set of volumes that failed disk checks.
     */
    void call(Set<FsVolumeSpi> healthyVolumes,
              Set<FsVolumeSpi> failedVolumes);
  }

  /**
   * Check a single volume, returning a {@link ListenableFuture}
   * that can be used to retrieve the final result.
   *
   * If the volume cannot be referenced then it is already closed and
   * cannot be checked. No error is propagated to the callback.
   *
   * @param volume the volume that is to be checked.
   * @param callback callback to be invoked when the volume check completes.
   * @return true if the check was scheduled and the callback will be invoked.
   *         false otherwise.
   */
  public boolean checkVolume(
      final FsVolumeSpi volume,
      Callback callback) {
    FsVolumeReference volumeReference;
    try {
      volumeReference = volume.obtainReference();
    } catch (ClosedChannelException e) {
      // The volume has already been closed.
      callback.call(new HashSet<>(), new HashSet<>());
      return false;
    }
    ListenableFuture<VolumeCheckResult> future =
        delegateChecker.schedule(volume, IGNORED_CONTEXT);
    numVolumeChecks.incrementAndGet();
    Futures.addCallback(future, new ResultHandler(
        volumeReference, new HashSet<>(), new HashSet<>(),
        new AtomicLong(1), callback));
    return true;
  }

  /**
   * A callback to process the results of checking a volume.
   */
  private class ResultHandler
      implements FutureCallback<VolumeCheckResult> {
    private final FsVolumeReference reference;
    private final Set<FsVolumeSpi> failedVolumes;
    private final Set<FsVolumeSpi> healthyVolumes;
    private final AtomicLong volumeCounter;

    @Nullable
    private final Callback callback;

    /**
     *
     * @param reference FsVolumeReference to be released when the check is
     *                  complete.
     * @param healthyVolumes set of healthy volumes. If the disk check is
     *                       successful, add the volume here.
     * @param failedVolumes set of failed volumes. If the disk check fails,
     *                      add the volume here.
     * @param semaphore semaphore used to trigger callback invocation.
     * @param callback invoked when the semaphore can be successfully acquired.
     */
    ResultHandler(FsVolumeReference reference,
                  Set<FsVolumeSpi> healthyVolumes,
                  Set<FsVolumeSpi> failedVolumes,
                  AtomicLong volumeCounter,
                  @Nullable Callback callback) {
      Preconditions.checkState(reference != null);
      this.reference = reference;
      this.healthyVolumes = healthyVolumes;
      this.failedVolumes = failedVolumes;
      this.volumeCounter = volumeCounter;
      this.callback = callback;
    }

    @Override
    public void onSuccess(@Nonnull VolumeCheckResult result) {
      switch(result) {
      case HEALTHY:
      case DEGRADED:
        LOG.debug("Volume {} is {}.", reference.getVolume(), result);
        markHealthy();
        break;
      case FAILED:
        LOG.warn("Volume {} detected as being unhealthy",
            reference.getVolume());
        markFailed();
        break;
      default:
        LOG.error("Unexpected health check result {} for volume {}",
            result, reference.getVolume());
        markHealthy();
        break;
      }
      cleanup();
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
      Throwable exception = (t instanceof ExecutionException) ?
          t.getCause() : t;
      LOG.warn("Exception running disk checks against volume " +
          reference.getVolume(), exception);
      markFailed();
      cleanup();
    }

    private void markHealthy() {
      synchronized (DatasetVolumeChecker.this) {
        healthyVolumes.add(reference.getVolume());
      }
    }

    private void markFailed() {
      synchronized (DatasetVolumeChecker.this) {
        failedVolumes.add(reference.getVolume());
      }
    }

    private void cleanup() {
      IOUtils.cleanup(null, reference);
      invokeCallback();
    }

    private void invokeCallback() {
      try {
        final long remaining = volumeCounter.decrementAndGet();
        if (callback != null && remaining == 0) {
          callback.call(healthyVolumes, failedVolumes);
        }
      } catch(Exception e) {
        // Propagating this exception is unlikely to be helpful.
        LOG.warn("Unexpected exception", e);
      }
    }
  }

  /**
   * Shutdown the checker and its associated ExecutorService.
   *
   * See {@link ExecutorService#awaitTermination} for the interpretation
   * of the parameters.
   */
  public void shutdownAndWait(int gracePeriod, TimeUnit timeUnit) {
    try {
      delegateChecker.shutdownAndWait(gracePeriod, timeUnit);
    } catch (InterruptedException e) {
      LOG.warn("DatasetVolumeChecker interrupted during shutdown.");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * This method is for testing only.
   *
   * @param testDelegate
   */
  @VisibleForTesting
  void setDelegateChecker(
      AsyncChecker<VolumeCheckContext, VolumeCheckResult> testDelegate) {
    delegateChecker = testDelegate;
  }

  /**
   * Return the number of {@link #checkVolume} invocations.
   */
  public long getNumVolumeChecks() {
    return numVolumeChecks.get();
  }

  /**
   * Return the number of {@link #checkAllVolumes} invocations.
   */
  public long getNumSyncDatasetChecks() {
    return numSyncDatasetChecks.get();
  }

  /**
   * Return the number of {@link #checkAllVolumesAsync} invocations.
   */
  public long getNumAsyncDatasetChecks() {
    return numAsyncDatasetChecks.get();
  }

  /**
   * Return the number of checks skipped because the minimum gap since the
   * last check had not elapsed.
   */
  public long getNumSkippedChecks() {
    return numSkippedChecks.get();
  }
}
