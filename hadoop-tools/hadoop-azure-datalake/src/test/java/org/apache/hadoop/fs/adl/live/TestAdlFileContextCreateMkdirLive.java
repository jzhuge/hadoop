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

import java.util.UUID;

import org.apache.hadoop.fs.FileContextCreateMkdirBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * Test file context Create/Mkdir operation.
 */
public class TestAdlFileContextCreateMkdirLive
    extends FileContextCreateMkdirBaseTest {

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
    return new FileContextTestHelper(UUID.randomUUID().toString());
  }
}
