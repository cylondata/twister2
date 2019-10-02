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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.zk.ZKJobsMonitor;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;

public final class ZKJobsMonitorExample {
  public static final Logger LOG = Logger.getLogger(ZKJobsMonitorExample.class.getName());

  public static Config config;

  private ZKJobsMonitorExample() { }

  public static void main(String[] args) {
    if (args.length < 1) {
      printUsage();
      return;
    }

    String zkAddress = args[0];
    config = buildTestConfig(zkAddress);

    ZKJobsMonitor jobsMonitor = new ZKJobsMonitor(config);
    try {
      jobsMonitor.initialize();
      LOG.info("Started ZKJobsMonitor.");
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      while (true) {
        Thread.sleep(300 * 1000);
        LOG.info("Monitoring jobs .....");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }


  /**
   * construct a test Config object
   */
  public static Config buildTestConfig(String zkAddresses) {

    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_ADDRESSES, zkAddresses)
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKJobsMonitorExample zkAddress\n"
        + "\tzkAddress is in the form of IP:PORT");
  }


}
