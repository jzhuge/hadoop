/**
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
 */
package org.apache.hadoop.fs.http.server;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test {@link HttpFSServerWebServer}.
 */
public class TestHttpFSServerWebServer {

  @Rule
  public Timeout timeout = new Timeout(30000);
  private HttpFSServerWebServer webServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    File homeDir = GenericTestUtils.getTestDir();
    File confDir = new File(homeDir, "etc/hadoop");
    File logsDir = new File(homeDir, "logs");
    File tempDir = new File(homeDir, "temp");
    confDir.mkdirs();
    logsDir.mkdirs();
    tempDir.mkdirs();
    System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath());
    System.setProperty("hadoop.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.home.dir", homeDir.getAbsolutePath());
    System.setProperty("httpfs.log.dir", logsDir.getAbsolutePath());
    System.setProperty("httpfs.config.dir", confDir.getAbsolutePath());
    new File(confDir, "httpfs-signature.secret").createNewFile();
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    Configuration sslConf = new Configuration();
    webServer = new HttpFSServerWebServer(conf, sslConf);
  }

  @Test
  public void testStartStop() throws Exception {
    webServer.start();
    webServer.stop();
  }

  @Test
  public void testJustStop() throws Exception {
    webServer.stop();
  }

  @Test
  public void testDoubleStop() throws Exception {
    webServer.start();
    webServer.stop();
    webServer.stop();
  }

  @Test
  public void testDoubleStart() throws Exception {
    webServer.start();
    webServer.start();
    webServer.stop();
  }

}
