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
package edu.iu.dsc.tws.examples.batch.sort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BKeyedPartition;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class SortJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(SortJob.class.getName());

  private BKeyedPartition partition;

  private Communicator channel;

  private static final int NO_OF_TASKS = 4;

  private Config config;

  private int workerId;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private TaskPlan taskPlan;
  private List<Integer> taskStages = new ArrayList<>();
  private Set<RecordSource> recordSources = new HashSet<>();

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.config = cfg;
    this.workerId = workerID;

    taskStages.add(NO_OF_TASKS);
    taskStages.add(NO_OF_TASKS);
    List<JobMasterAPI.WorkerInfo> workerList = workerController.waitForAllWorkersToJoin(50000);
    // lets create the task plan
    this.taskPlan = Utils.createStageTaskPlan(cfg, workerID,
        taskStages, workerList);

    // setup the network
    setupNetwork(workerController);
    // set up the tasks
    setupTasks();

    partition = new BKeyedPartition(channel, taskPlan, sources, destinations,
        MessageType.BYTE, MessageType.BYTE, MessageType.INTEGER, MessageType.INTEGER,
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
        taskPlan.getThisExecutor(), sources, destinations));
  }

  private void scheduleTasks() {
    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan, taskStages, 0);
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

  private void setupNetwork(IWorkerController controller) {
    TWSChannel twsChannel = Network.initializeChannel(config, controller);
    this.channel = new Communicator(config, twsChannel);
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
      channel.getChannel().progress();

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
