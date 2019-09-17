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
package edu.iu.dsc.tws.examples.batch.wordcount.comms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BKeyedReduce;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.examples.Utils;

public class WordCountWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(WordCountWorker.class.getName());

  private BKeyedReduce keyGather;

  private static final int NO_OF_TASKS = 8;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private LogicalPlan logicalPlan;

  private Set<BatchWordSource> batchWordSources = new HashSet<>();

  private WordAggregator wordAggregator;
  private List<Integer> taskStages = new ArrayList<>();
  private int workerId;
  private WorkerEnvironment workerEnv;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.workerId = workerID;

    taskStages.add(NO_OF_TASKS);
    taskStages.add(NO_OF_TASKS);

    // create a worker environment
    this.workerEnv = WorkerEnvironment.init(cfg, workerID, workerController, persistentVolume,
        volatileVolume);

    // lets create the task plan
    this.logicalPlan = Utils.createStageLogicalPlan(workerEnv, taskStages);

    setupTasks();

    // create the communication
    wordAggregator = new WordAggregator();
    keyGather = new BKeyedReduce(workerEnv.getCommunicator(), logicalPlan, sources, destinations,
        new ReduceFunction() {
          @Override
          public void init(Config cfg, DataFlowOperation op, Map<Integer,
              List<Integer>> expectedIds) {

          }

          @Override
          public Object reduce(Object t1, Object t2) {
            return (Integer) t1 + (Integer) t2;
          }
        },
        wordAggregator, MessageTypes.OBJECT, MessageTypes.INTEGER, new HashingSelector());
    // assign the task ids to the workers, and run them using threads
    scheduleTasks();
    // progress the communication
    progress();

    // close communication
    workerEnv.close();
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
        logicalPlan.getThisWorker(), sources, destinations));
  }

  private void scheduleTasks() {
    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, logicalPlan,
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
      workerEnv.getChannel().progress();

      // we should communicationProgress the communication directive
      boolean needsProgress = keyGather.progress();
      if (needsProgress) {
        done = false;
      }

      if (!keyGather.isComplete()) {
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
