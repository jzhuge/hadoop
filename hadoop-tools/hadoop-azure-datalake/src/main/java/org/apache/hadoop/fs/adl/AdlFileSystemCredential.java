package org.apache.hadoop.fs.adl;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystemCredential;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add a class comment here
 */
public class AdlFileSystemCredential extends Configured
    implements FileSystemCredential {
  public static final Logger LOG =
      LoggerFactory.getLogger(AdlFileSystemCredential.class);
  private static boolean loaded = false;

  @Override
  public void load(UserGroupInformation loginUser) throws IOException {
    synchronized (AdlFileSystemCredential.class) {
      if (!loaded) {
        loaded = true;
        loadAccessTokensFile(loginUser,
            AzureADAccessToken.getAccessTokensFile());
      }
    }
  }
  
  void loadAccessTokensFile(UserGroupInformation loginUser,
                            File accessTokensFile) throws IOException {
    if (!accessTokensFile.canRead()) {
      return;
    }
    AzureADAccessToken[] accessTokens = new ObjectMapper().readValue(
        accessTokensFile, AzureADAccessToken[].class);
    if (accessTokens.length == 0) {
      return;
    }
    AzureADAccessToken token = null;
    // Search for a token of RefreshToken type backwards from the latest.
    for (int i = accessTokens.length - 1; i >= 0; --i) {
      if (accessTokens[i].getRefreshToken() != null) {
        token = accessTokens[i];
        break;
      }
    }
    if (token != null) {
      LOG.info("Loading Azure refresh token for " + token.getUserId());
      Credentials azureCredentials = new Credentials();
      azureCredentials.addSecretKey(Configuration.nameToAlias(
          AdlConfKeys.AZURE_AD_TOKEN_PROVIDER_TYPE_KEY),
          "RefreshToken".getBytes());
      azureCredentials.addSecretKey(Configuration.nameToAlias(
          AdlConfKeys.AZURE_AD_CLIENT_ID_KEY),
          token.get_clientId().getBytes());
      azureCredentials.addSecretKey(Configuration.nameToAlias(
          AdlConfKeys.AZURE_AD_REFRESH_TOKEN_KEY),
          token.getRefreshToken().getBytes());
      loginUser.addCredentials(azureCredentials);
    }
  }
}
