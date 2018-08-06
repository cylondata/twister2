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
import edu.iu.dsc.tws.rsched.bootstrap.ZKController;
import edu.iu.dsc.tws.rsched.bootstrap.ZKUtil;

public final class ZKControllerExample {
  public static final Logger LOG = Logger.getLogger(ZKControllerExample.class.getName());

  private ZKControllerExample() { }

  /**
   * example usage of ZKController class
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

    String zkAddress = args[0];
    String action = args[1];
    String jobName = "test-job";

    Config cnfg = buildTestConfig(zkAddress, jobName);

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
  public static Config buildTestConfig(String zkAddresses, String jobName) {

    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_ADDRESSES, zkAddresses)
        .put(Context.JOB_NAME, jobName)
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKControllerExample zkAddress action numberOfWorkers\n"
        + "\tzkAddress is in the form of IP:PORT"
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

  /**
   * an example usage of ZKController class
   * @param jobName
   * @param numberOfWorkers
   * @param cnfg
   */
  public static void simulateWorker(String jobName, int numberOfWorkers, Config cnfg) {
    int port = 1000 + (int) (Math.random() * 1000);
    String workerAddress = "localhost:" + port;

    ZKController zkController = new ZKController(cnfg, jobName, workerAddress, numberOfWorkers);
    zkController.initialize();

    List<WorkerNetworkInfo> workerList = zkController.getWorkerList();
    LOG.info("Initial worker list: \n" + WorkerNetworkInfo.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = zkController.waitForAllWorkersToJoin(100000);
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    sleeeep((long) (Math.random() * 10000));

    LOG.info("Waiting on the first barrier -------------------------- ");
    long timeLimit = 200000;
    boolean allWorkersReachedBarrier = zkController.waitOnBarrier(timeLimit);
    if (allWorkersReachedBarrier) {
      LOG.info("All workers reached the barrier. Proceeding.");
    } else {
      LOG.info("Not all workers reached the barrier on the given timelimit: " + timeLimit + "ms"
          + " Exiting ....... ");
      zkController.close();
      return;
    }

    sleeeep((long) (Math.random() * 10000));

    LOG.info("Waiting on the second barrier -------------------------- ");
    allWorkersReachedBarrier = zkController.waitOnBarrier(timeLimit);
    if (allWorkersReachedBarrier) {
      LOG.info("All workers reached the barrier. Proceeding.");
    } else {
      LOG.info("Not all workers reached the barrier on the given timelimit: " + timeLimit + "ms"
          + " Exiting ....... ");
      zkController.close();
      return;
    }

    // sleep some random amount of time before closing
    // this is to prevent all workers to close almost at the same time
    sleeeep((long) (Math.random() * 2000));
    zkController.close();
  }

  public static void sleeeep(long duration) {

    LOG.info("Sleeping " + duration + "ms .....");

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
