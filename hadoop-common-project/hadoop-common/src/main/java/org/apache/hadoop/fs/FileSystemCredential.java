package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * Add a class comment here
 */
public interface FileSystemCredential {
  void load(UserGroupInformation loginUser) throws IOException;
}