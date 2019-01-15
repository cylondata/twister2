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
package edu.iu.dsc.tws.examples.comms.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Iterators;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BDirect;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;

public class BDirectExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BDirectExample.class.getName());

  private BDirect direct;

  private boolean partitionDone = false;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    List<Integer> sources = new ArrayList<>();
    List<Integer> targets = new ArrayList<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    // create the communication
    direct = new BDirect(communicator, taskPlan, sources, targets,
        new DirectReceiver(), MessageType.INTEGER);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    direct.close();
  }

  @Override
  protected void progressCommunication() {
    direct.progress();
  }

  @Override
  protected boolean isDone() {
    return partitionDone && sourcesDone && !direct.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!direct.direct(task, data, flag)) {
      // lets wait a litte and try again
      direct.progress();
    }
    return true;
  }

  public class DirectReceiver implements BulkReceiver {
    private int count = 0;
    private int expected;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      expected = jobParameters.getIterations();
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      LOG.log(Level.INFO, String.format("%d Received message %d count %d expected %d",
          workerId, target, Iterators.size(it), expected));
      partitionDone = true;
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    direct.finish(src);
  }
}
