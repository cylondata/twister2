//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.examples.internal.bootstrap;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterFinder;

public final class ZKJobMasterFinderExample {
  private static final Logger LOG = Logger.getLogger(ZKJobMasterFinderExample.class.getName());

  private ZKJobMasterFinderExample() { }

  /**
   * This class is used together with ZKJobMasterRegistrarExample.java
   * That class registers the Job Master and this class discovers it
   *
   * This class tries to get the Job Master address from a ZooKeeper server
   * If the Job Master has not been registered yet,
   * it can wait for it to be registered
   *
   */
  public static void main(String[] args) {

    if (args.length != 1) {
      printUsage();
      return;
    }

    String zkAddress = args[0];
    String jobName = "test-job";
    Config cnfg = buildTestConfig(zkAddress, jobName);

    ZKJobMasterFinder finder = new ZKJobMasterFinder(cnfg);
    finder.initialize();

    String jobMasterIPandPort = finder.getJobMasterIPandPort();
    if (jobMasterIPandPort == null) {
      LOG.info("Job Master has not joined yet. Will wait and try to get the address ...");
      jobMasterIPandPort = finder.waitAndGetJobMasterIPandPort(20000);
      LOG.info("Job Master address: " + jobMasterIPandPort);
    } else {
      LOG.info("Job Master address: " + jobMasterIPandPort);
    }

    finder.close();
    LOG.info("Done, exiting ...");
  }

  /**
   * construct a test Config object
   * @return
   */
  public static Config buildTestConfig(String zkAddress, String jobName) {
    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_ADDRESSES, zkAddress)
        .put(Context.JOB_NAME, jobName)
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKJobMasterFinderExample zkAddress");
  }

}
