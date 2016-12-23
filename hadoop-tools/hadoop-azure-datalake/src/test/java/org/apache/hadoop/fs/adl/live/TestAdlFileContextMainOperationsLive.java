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

import static org.apache.hadoop.util.Shell.WINDOWS;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Run collection of tests for the {@link org.apache.hadoop.fs.FileContext}.
 */
public class TestAdlFileContextMainOperationsLive
    extends FileContextMainOperationsBaseTest {

  @BeforeClass
  public static void skipTestCheck() {
    Assume.assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  @Override
  public void setUp() throws Exception {
    fc = AdlStorageConfiguration.createFileContext();
    super.setUp();
  }

  @Override
  protected FileContextTestHelper createFileContextHelper() {
    // On Windows, root directory path is created from local running directory.
    // Adl does not support ':' as part of the path which results in failure.
    //    return new FileContextTestHelper(GenericTestUtils
    // .getRandomizedTestDir()
    //        .getAbsolutePath().replaceAll(":",""));
    return new FileContextTestHelper(UUID.randomUUID().toString());
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Override
  public void testWorkingDirectory() throws Exception {
    if (WINDOWS) {
      // TODO :Fix is required in Hadoop shell to support windows permission
      // set.
      // The test is failing with NPE on windows platform only, with Linux
      // platform test passes.
      Assume.assumeTrue(false);
    } else {
      super.testWorkingDirectory();
    }
  }

  @Override
  public void testUnsupportedSymlink() throws IOException {
    Assume.assumeTrue(false);
  }

  @Test
  public void testSetVerifyChecksum() throws IOException {
    Assume.assumeTrue(false);
  }
}