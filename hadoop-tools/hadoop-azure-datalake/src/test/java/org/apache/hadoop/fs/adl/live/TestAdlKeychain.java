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
package org.apache.hadoop.fs.adl.live;

import junit.framework.AssertionFailedError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.tracing.SetSpanReceiver;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.core.AlwaysSampler;
import org.apache.htrace.core.Tracer;
import org.junit.Assert;
import org.junit.Test;

public class TestAdlKeychain {

  @Test
  public void testLs() throws Throwable {
    Configuration conf = new Configuration();
    FsShell shell = new FsShell(conf);
    ToolRunner.run(shell,
        new String[]{"-Dadl.feature.ownerandgroup.enableupn=true", "-ls",
            "adl://jzhugeadls.azuredatalakestore.net/"});
  }

}
