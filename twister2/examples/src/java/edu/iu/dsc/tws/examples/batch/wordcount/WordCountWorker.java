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
package edu.iu.dsc.tws.examples.batch.wordcount;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BKeyedReduce;
import edu.iu.dsc.tws.comms.op.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class WordCountWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(WordCountWorker.class.getName());

  private BKeyedReduce keyGather;

  private Communicator channel;

  private static final int NO_OF_TASKS = 8;

  private Config config;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private TaskPlan taskPlan;

  private Set<BatchWordSource> batchWordSources = new HashSet<>();

  private WordAggregator wordAggregator;
  private List<Integer> taskStages = new ArrayList<>();
  private int workerId;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.config = cfg;
    this.workerId = workerID;

    taskStages.add(NO_OF_TASKS);
    taskStages.add(NO_OF_TASKS);

    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (java.util.concurrent.TimeoutException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return;
    }
    // lets create the task plan
    this.taskPlan = Utils.createStageTaskPlan(
        cfg, workerID, taskStages, workerList);

    setupTasks();
    setupNetwork(workerController);

    // create the communication
    wordAggregator = new WordAggregator();
    keyGather = new BKeyedReduce(channel, taskPlan, sources, destinations,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER),
        wordAggregator, MessageType.OBJECT, MessageType.INTEGER, new HashingSelector());
    // assign the task ids to the workers, and run them using threads
    scheduleTasks();
    // progress the communication
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

  private void setupNetwork(IWorkerController controller) {
    TWSChannel twsChannel = Network.initializeChannel(config, controller);
    this.channel = new Communicator(config, twsChannel);
  }

  private void scheduleTasks() {
    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        taskStages, 0);
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      BatchWordSource target = new BatchWordSource(keyGather, 1000, t, 10);
      batchWordSources.add(target);
      Thread mapThread = new Thread(target);
      mapThread.start();
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
      boolean needsProgress = keyGather.progress();
      if (needsProgress) {
        done = false;
      }

      if (keyGather.hasPending()) {
        done = false;
      }

      for (BatchWordSource b : batchWordSources) {
        if (!b.isDone()) {
          done = false;
        }
      }
      if (!wordAggregator.isDone()) {
        done = false;
      }
    }
  }
}
