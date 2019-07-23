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
package edu.iu.dsc.tws.examples.batch.sortop;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.comms.batch.BKeyedPartition;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class SortJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(SortJob.class.getName());

  private BKeyedPartition partition;

  private static final int NO_OF_TASKS = 4;

  private int workerId;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private LogicalPlan logicalPlan;
  private List<Integer> taskStages = new ArrayList<>();
  private Set<RecordSource> recordSources = new HashSet<>();
  private WorkerEnvironment workerEnv;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.workerId = workerID;

    // create a worker environment & setup the network
    this.workerEnv = WorkerEnvironment.init(cfg, workerID, workerController, persistentVolume,
        volatileVolume);

    taskStages.add(NO_OF_TASKS);
    taskStages.add(NO_OF_TASKS);
    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    // lets create the task plan
    this.logicalPlan = Utils.createStageLogicalPlan(workerEnv, taskStages);

    // set up the tasks
    setupTasks();

    partition = new BKeyedPartition(workerEnv.getCommunicator(), logicalPlan, sources, destinations,
        MessageTypes.INTEGER, MessageTypes.BYTE_ARRAY,
        new RecordSave(), new HashingSelector(), new IntegerComparator());

    // start the threads
    scheduleTasks();
    progress();
  }

  private void setupTasks() {
    sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    destinations = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      destinations.add(NO_OF_TASKS + i);
    }
    LOG.fine(String.format("%d sources %s destinations %s",
        logicalPlan.getThisExecutor(), sources, destinations));
  }

  private void scheduleTasks() {
    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, logicalPlan, taskStages, 0);
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      RecordSource target = new RecordSource(workerId, partition, t, 1000, 10000);
      // the map thread where data is produced
      recordSources.add(target);
      Thread mapThread = new Thread(target);
      mapThread.start();
    }
  }

  private class IntegerComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      int o11 = (int) o1;
      int o21 = (int) o2;
      return Integer.compare(o11, o21);
    }
  }

  private void progress() {
    // we need to communicationProgress the communication
    boolean done = false;
    while (!done) {
      done = true;
      // communicationProgress the channel
      workerEnv.getChannel().progress();

      // we should communicationProgress the communication directive
      boolean needsProgress = partition.progress();
      if (needsProgress) {
        done = false;
      }

      if (partition.hasPending()) {
        done = false;
      }

      for (RecordSource b : recordSources) {
        if (!b.isDone()) {
          done = false;
        }
      }
    }
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("sort-job");
    jobBuilder.setWorkerClass(SortJob.class.getName());
    jobBuilder.addComputeResource(1, 512, NO_OF_TASKS);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
