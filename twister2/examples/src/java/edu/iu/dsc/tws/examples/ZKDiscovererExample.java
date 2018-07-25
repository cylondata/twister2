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
package edu.iu.dsc.tws.examples;

import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKDiscoverer;
import edu.iu.dsc.tws.rsched.bootstrap.ZKUtil;

public final class ZKDiscovererExample {
  public static final Logger LOG = Logger.getLogger(ZKDiscovererExample.class.getName());

  private ZKDiscovererExample() { }

  /**
   * example usage of ZKDiscoverer class
   * Two actions supported:
   *   join: join a Job znode
   *   delete: delete a Job znode
   *
   * First parameter is the ZooKeeper server address
   * Second parameter is the requested action
   * Third parameter is the numberOfWorkers to join the job
   * Third parameter is used for join, not for delete
   *
   * Sometimes, two workers may finish almost at the same time
   * in that case, both worker caches may not be updated
   * therefore, both think that they are not the last ones
   * so, the job znode may not be deleted
   * in that case, delete action can be used to delete the previous job znode
   */
  public static void main(String[] args) {

    if (args.length < 2) {
      printUsage();
      return;
    }

    String ip = args[0];
    String action = args[1];
    String jobName = "test-job";
    String port = "2181";

    Config cnfg = buildTestConfig(ip, port, jobName);

    if ("delete".equalsIgnoreCase(action)) {
      deleteJobZnode(jobName, cnfg);
    } else if ("join".equalsIgnoreCase(action)) {
      int numberOfWorkers = Integer.parseInt(args[2]);
      simulateWorker(jobName, numberOfWorkers, cnfg);
    }

  }

  /**
   * construct a test Config object
   * @return
   */
  public static Config buildTestConfig(String ip, String port, String jobName) {
    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_IP, ip)
        .put(ZKContext.ZOOKEEPER_SERVER_PORT, port)
        .put(Context.JOB_NAME, jobName)
        .build();
  }

  public static void sleeeep(long duration) {

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKDiscovererExample zkAddress action numberOfWorkers\n"
        + "\taction can be: join, delete\n"
        + "\tnumberOfWorkers is not needed for delete");
  }

  public static void deleteJobZnode(String jobName, Config cnfg) {
    if (ZKUtil.isThereAnActiveJob(jobName, cnfg)) {
      ZKUtil.terminateJob(jobName, cnfg);
      return;
    } else {
      LOG.info("No job Znode to delete for the jobName: " + jobName);
      return;
    }
  }

  public static void simulateWorker(String jobName, int numberOfWorkers, Config cnfg) {
    int port = 1000 + (int) (Math.random() * 1000);
    String workerAddress = "localhost:" + port;

    ZKDiscoverer zkController = new ZKDiscoverer(cnfg, jobName, workerAddress, numberOfWorkers);
    zkController.initialize();

    List<WorkerNetworkInfo> workerList = zkController.getWorkerList();
    LOG.info("Initial worker list: \n" + WorkerNetworkInfo.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = zkController.waitForAllWorkersToJoin(100000);
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    sleeeep((long) (Math.random() * 1000));

    zkController.close();
  }

}
