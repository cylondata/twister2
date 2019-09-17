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
package edu.iu.dsc.tws.examples.ml.svm.comms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BReduce;
import edu.iu.dsc.tws.comms.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.examples.Utils;

public class SVMCommsReducer extends CommsWorker {

  private static final Logger LOG = Logger.getLogger(SVMCommsReducer.class.getName());

  private BReduce reduce;

  private boolean reduceDone;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = svmJobParameters.getParallelism();
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;
    reduce = new BReduce(workerEnv.getCommunicator(), logicalPlan, sources, target,
        new ReduceOperationFunction(Op.SUM, MessageTypes.DOUBLE), new FinalSingularReceiver(),
        MessageTypes.DOUBLE);
    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, logicalPlan,
        taskStages, 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!logicalPlan.getLogicalIdsOfWorker(workerId).contains(target)) {
      reduceDone = true;
    }

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, target, tasksOfExecutor));
    for (int t : tasksOfExecutor) {
      Thread mapThread = new Thread(new DataStreamer(t));
      mapThread.start();
    }

  }

  @Override
  protected void progressCommunication() {
    reduce.progress();
  }

  @Override
  protected boolean isDone() {
    return reduceDone && sourcesDone && reduce.isComplete();
  }

  @Override
  public void close() {
    reduce.close();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!reduce.reduce(task, data, flag)) {
      // lets wait a litte and try again
      reduce.progress();
    }
    return true;
  }

  @Override
  protected void finishCommunication(int src) {
    reduce.finish(src);
  }

  @Override
  public List<Integer> generateTaskStages() {
    if (taskStages != null) {
      taskStages.clear();
    }

    if (taskStages == null) {
      taskStages = new ArrayList<>(2);
    }

    taskStages.add(0, this.svmJobParameters.getParallelism());
    taskStages.add(1, 1);
    return taskStages;
  }

  public class FinalSingularReceiver implements SingularReceiver {

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      if (targets.isEmpty()) {
        reduceDone = true;
        return;
      }
    }

    @Override
    public boolean receive(int target, Object object) {
      LOG.info(String.format("Target %d, Object : %s", target, object.getClass().getName()));
      if (object instanceof double[]) {
        LOG.info(String.format("Data Received : " + Arrays.toString((double[]) object)));
      }
      reduceDone = true;
      return true;
    }
  }
}
