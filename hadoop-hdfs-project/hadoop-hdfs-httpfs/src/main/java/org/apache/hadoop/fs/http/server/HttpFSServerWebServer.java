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

import static org.apache.hadoop.util.StringUtils.startupShutdownMessage;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.ConfigurationWithLogging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HttpFS web server.
 */
@InterfaceAudience.Private
public class HttpFSServerWebServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(HttpFSServerWebServer.class);

  private static final String HTTPFS_DEFAULT_XML = "httpfs-default.xml";
  private static final String HTTPFS_SITE_XML = "httpfs-site.xml";

  // HTTP properties
  private static final String HTTP_PORT_KEY = "hadoop.httpfs.http.port";
  private static final int HTTP_PORT_DEFAULT = 14000;
  private static final String HTTP_HOST_KEY = "hadoop.httpfs.http.host";
  private static final String HTTP_HOST_DEFAULT = "0.0.0.0";

  // SSL properties
  private static final String SSL_ENABLED_KEY = "hadoop.httpfs.ssl.enabled";
  private static final boolean SSL_ENABLED_DEFAULT = false;

  private static final String NAME = "webhdfs";

  static {
    Configuration.addDefaultResource(HTTPFS_DEFAULT_XML);
    Configuration.addDefaultResource(HTTPFS_SITE_XML);
  }

  private final HttpServer2 httpServer;
  private final String scheme;

  HttpFSServerWebServer(Configuration cnf) throws Exception {
    ConfigurationWithLogging conf = new ConfigurationWithLogging(cnf);

    // Add SSL configuration
    conf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY,
        SSLFactory.SSL_SERVER_CONF_DEFAULT));

    // Override configuration with deprecated environment variables.
    deprecateEnv("HTTPFS_TEMP", conf, HttpServer2.HTTP_TEMP_DIR_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_HTTP_PORT", conf, HTTP_PORT_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_THREADS", conf,
        HttpServer2.HTTP_MAX_THREADS_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_REQUEST_HEADER_SIZE_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_MAX_HTTP_HEADER_SIZE", conf,
        HttpServer2.HTTP_MAX_RESPONSE_HEADER_SIZE_KEY, HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_SSL_ENABLED", conf, SSL_ENABLED_KEY,
        HTTPFS_SITE_XML);
    deprecateEnv("HTTPFS_SSL_KEYSTORE_FILE", conf,
        SSLFactory.SSL_SERVER_KEYSTORE_LOCATION,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);
    deprecateEnv("HTTPFS_SSL_KEYSTORE_PASS", conf,
        SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD,
        SSLFactory.SSL_SERVER_CONF_DEFAULT);

    boolean sslEnabled = conf.getBoolean(SSL_ENABLED_KEY,
        SSL_ENABLED_DEFAULT);
    scheme = sslEnabled ? HttpServer2.HTTPS_SCHEME : HttpServer2.HTTP_SCHEME;

    String host = conf.get(HTTP_HOST_KEY, HTTP_HOST_DEFAULT);
    int port = conf.getInt(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
    URI endpoint = new URI(scheme, null, host, port, null, null, null);

    httpServer = new HttpServer2.Builder()
        .setName(NAME)
        .setConf(conf)
        .setSSLConf(conf)
        .authFilterConfigurationPrefix(HttpFSAuthenticationFilter.CONF_PREFIX)
        .addEndpoint(endpoint)
        .build();
  }

  /**
   * Load the deprecated environment variable into the configuration.
   *
   * @param varName the environment variable name
   * @param conf the configuration
   * @param propName the configuration property name
   * @param confFile the configuration file name
   */
  private static void deprecateEnv(String varName, Configuration conf,
                                   String propName, String confFile) {
    String value = System.getenv(varName);
    if (value == null) {
      return;
    }
    String propValue = conf.get(propName);
    LOG.warn("Environment variable {} = '{}' is deprecated and overriding"
            + " property {} = '{}', please set the property in {} instead.",
        varName, value, propName, propValue, confFile);
    conf.set(propName, value, "environment variable " + varName);
  }

  public void start() throws IOException {
    httpServer.start();
  }

  public void join() throws InterruptedException {
    httpServer.join();
  }

  public void stop() throws Exception {
    httpServer.stop();
  }

  public static void main(String[] args) throws Exception {
    startupShutdownMessage(HttpFSServerWebServer.class, args, LOG);
    Configuration conf = new ConfigurationWithLogging(
        new Configuration(true));
    HttpFSServerWebServer webServer = new HttpFSServerWebServer(conf);
    webServer.start();
    webServer.join();
  }
}
