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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.worker.WorkerEnv;
import edu.iu.dsc.tws.comms.batch.BDirect;
import edu.iu.dsc.tws.examples.Utils;

public class InputDataStreamer extends CommsWorker {

  private static final Logger LOG = Logger.getLogger(InputDataStreamer.class.getName());

  private BDirect direct;

  private boolean directDone = false;


  @Override
  protected void execute(WorkerEnv workerEnv) {
    List<Integer> sources = new ArrayList<>();
    List<Integer> targets = new ArrayList<>();
    Integer noOfSourceTasks = taskStages.get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = taskStages.get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    direct = new BDirect(workerEnv.getCommunicator(), logicalPlan, sources, targets,
        new DirectReceiver(), MessageTypes.DOUBLE);


    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, logicalPlan,
        taskStages, 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }
    for (int t : tasksOfExecutor) {
      Thread mapThread = new Thread(new DataStreamer(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    direct.progress();
  }

  @Override
  protected boolean isDone() {
    return directDone && sourcesDone && !direct.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!direct.direct(task, data, flag)) {
      // lets wait a litte and try again
      direct.progress();
    }
    return true;
  }

  @Override
  public List<Integer> generateTaskStages() {
    if (taskStages == null) {
      taskStages = new ArrayList<>(2);
    } else {
      taskStages.clear();
    }
    taskStages.add(0, this.svmJobParameters.getParallelism());
    taskStages.add(1, this.svmJobParameters.getParallelism());
    return taskStages;
  }

  @Override
  protected void finishCommunication(int src) {
    direct.finish(src);
  }

  public class DirectReceiver implements BulkReceiver {

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      if (targets.isEmpty()) {
        directDone = true;
        return;
      }
    }

    @Override
    public boolean receive(int target, Iterator<Object> object) {
      directDone = true;
      while (object.hasNext()) {
        Object o = object.next();
        if (o instanceof double[]) {
          LOG.info(String.format("Received Data : %s", Arrays.toString((double[]) o)));
        } else {
          LOG.info(String.format("Received Data : %s", o.getClass().getName()));
        }
      }
      return true;
    }
  }
}
