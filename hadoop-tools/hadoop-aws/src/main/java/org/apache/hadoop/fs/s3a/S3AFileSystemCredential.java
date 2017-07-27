package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystemCredential;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add a class comment here
 */
public class S3AFileSystemCredential extends Configured
    implements FileSystemCredential {
  public static final Logger LOG =
      LoggerFactory.getLogger(S3AFileSystemCredential.class);
  private static boolean loaded = false;

  @Override
  public void load(UserGroupInformation loginUser) throws IOException {
    synchronized (S3AFileSystemCredential.class) {
      if (!loaded) {
        loaded = true;
        loadAuthKeys(loginUser, new File(String.format(
            "%s/.config/%s/auth-keys.xml", System.getProperty("user.home"),
            S3AFileSystem.SCHEME)));
      }
    }
  }

  void loadAuthKeys(UserGroupInformation loginUser,
                    File authKeysXml) throws IOException {
    if (!authKeysXml.canRead()) {
      return;
    }
    Configuration conf = new Configuration(false);
    conf.addResource(new Path(authKeysXml.getPath()));
    LOG.info("Loading S3A access key");
    Credentials awsCredentials = new Credentials();
    awsCredentials.addSecretKey(Configuration.nameToAlias(ACCESS_KEY),
        conf.get(ACCESS_KEY).getBytes());
    awsCredentials.addSecretKey(Configuration.nameToAlias(SECRET_KEY),
        conf.get(SECRET_KEY).getBytes());
    loginUser.addCredentials(awsCredentials);
  }
}
