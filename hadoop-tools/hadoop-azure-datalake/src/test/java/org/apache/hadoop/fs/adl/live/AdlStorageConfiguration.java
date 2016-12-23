/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.adl.AdlFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configure Adl storage file system.
 */
public final class AdlStorageConfiguration {

  private static final String KEY_FILE_SYSTEM_IMPL = "fs.contract.test.fs";
  private static final String KEY_FILE_SYSTEM =
      String.format("fs.contract.test.fs.%s", AdlFileSystem.SCHEME);

  private static boolean isContractTestEnabled = false;
  private static Configuration conf = null;

  private AdlStorageConfiguration() {
  }

  public synchronized static Configuration getConfiguration() {
    Configuration newConf = new Configuration();
    newConf.addResource(AdlStorageContract.CONTRACT_XML);
    return newConf;
  }

  public synchronized static boolean isContractTestEnabled() {
    if (conf == null) {
      conf = getConfiguration();
    }

    isContractTestEnabled = !conf.get(KEY_FILE_SYSTEM, "").isEmpty();
    return isContractTestEnabled;
  }

  public synchronized static FileSystem createStorageConnector()
      throws URISyntaxException, IOException {
    if (conf == null) {
      conf = getConfiguration();
    }

    if (!isContractTestEnabled()) {
      return null;
    }

    String fileSystem = conf.get(KEY_FILE_SYSTEM);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new IOException("Default file system not configured.");
    }
    String fileSystemImpl = conf.get(KEY_FILE_SYSTEM_IMPL);
    if (fileSystemImpl == null || fileSystemImpl.trim().length() == 0) {
      throw new IOException(
          "Configuration " + KEY_FILE_SYSTEM_IMPL + "does not exist.");
    }
    FileSystem fs = null;
    try {
      fs = (FileSystem) Class.forName(fileSystemImpl).newInstance();
    } catch (Exception e) {
      throw new IOException("Could not instantiate the filesystem.");
    }

    fs.initialize(new URI(conf.get(KEY_FILE_SYSTEM)), conf);
    return fs;
  }

  public static FileContext createFileContext() throws Exception {
    Configuration conf = AdlStorageConfiguration.getConfiguration();
    String fileSystem = conf.get(KEY_FILE_SYSTEM);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new Exception("Default file system not configured.");
    }
    URI uri = new URI(fileSystem);
    FileSystem fs = AdlStorageConfiguration.createStorageConnector();
    return FileContext.getFileContext(
        new DelegateToFileSystem(uri, fs, conf, fs.getScheme(), false) {
        }, conf);
  }
}
