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
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements IWorker, IAllJoinedListener {

  private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

  private List<JobMasterAPI.WorkerInfo> workerList;
  private Object waitObject = new Object();
  private long jobSubmitTime;
  private int workerID;

  @Override
  public void execute(Config config, int wID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    this.workerID = wID;
    jobSubmitTime = config.getLongValue("JOB_SUBMIT_TIME", -1);
    LOG.info("jobSubmitTime: " + jobSubmitTime);
    long workerStartTime = System.currentTimeMillis();
    LOG.info("timestamp workerStart: " + workerStartTime);
    LOG.severe("workerStartDelay: " + wID + " " + (workerStartTime - jobSubmitTime));

    boolean added = WorkerRuntime.addAllJoinedListener(this);
    if (!added) {
      LOG.warning("Can not register IAllJoinedListener.");
      waitAndComplete();
    }

    // lets wait for all workers to join the job
    if (workerList == null) {
      waitAllWorkersToJoin();
    }

    LOG.info("All workers joined. Worker IDs: " + getIDs(workerList));

    waitAndComplete();
  }

  private List<Integer> getIDs(List<JobMasterAPI.WorkerInfo> workers) {
    return workers.stream()
        .map(wi -> wi.getWorkerID())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * wait for all workers to join the job
   * this can be used for waiting initial worker joins or joins after scaling up the job
   */
  private void waitAllWorkersToJoin() {
    synchronized (waitObject) {
      try {
        LOG.info("Waiting for all workers to join the job... ");
        waitObject.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workers) {
    workerList = workers;
    long allJoinedTime = System.currentTimeMillis();
    LOG.info("timestamp allWorkersJoined: " + allJoinedTime);
    LOG.severe("allJoinedDelay: " + workerID + " " + (allJoinedTime - jobSubmitTime));

    synchronized (waitObject) {
      waitObject.notify();
    }
  }

  private void waitAndComplete() {

    long duration = 6000;
    try {
      LOG.info("Sleeping " + duration + " seconds. Will complete after that.");
      Thread.sleep(duration * 1000);
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
    jobConfig.put("JOB_SUBMIT_TIME", System.currentTimeMillis() + "");

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
