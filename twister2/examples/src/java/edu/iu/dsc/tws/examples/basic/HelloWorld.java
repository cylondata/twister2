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
package edu.iu.dsc.tws.examples.basic;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements IWorker {

  private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

  @Override
  public void execute(Config config, JobAPI.Job job,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    int workerID = workerController.getWorkerInfo().getWorkerID();
    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");

    // lets do a log to indicate we are running
    LOG.log(Level.INFO, String.format("Hello World from Worker %d; there are %d total workers "
            + "and I got a message: %s", workerID,
        workerController.getNumberOfWorkers(), helloKeyValue));

    // lets wait for all workers to join the job
    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    LOG.info("All workers joined. Worker IDs: " + getIDs(workerList));

    // lets sync with all workers
    LOG.info("Waiting on a barrier ........................ ");
    try {
      long start = System.currentTimeMillis();
      workerController.waitOnBarrier();
      long delay = System.currentTimeMillis() - start;
      LOG.info("Barrier wait time: " + delay + " ms for worker: " + workerID);
    } catch (TimeoutException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return;
    }

    LOG.info("Proceeded through the barrier ........................ ");

    waitSeconds(10);
  }

  private List<Integer> getIDs(List<JobMasterAPI.WorkerInfo> workerList) {
    return workerList.stream()
        .map(wi -> wi.getWorkerID())
        .sorted()
        .collect(Collectors.toList());
  }

  private void waitSeconds(long seconds) {

    try {
      LOG.info("Sleeping " + seconds + " seconds. Will complete after that.");
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // lets take number of workers as an command line argument
    int numberOfWorkers = 4;
    if (args.length == 1) {
      numberOfWorkers = Integer.valueOf(args[0]);
    }

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("hello-key", "Twister2-Hello");

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-world-job")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(.2, 128, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
