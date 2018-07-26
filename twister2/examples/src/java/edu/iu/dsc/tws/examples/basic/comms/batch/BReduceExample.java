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
package edu.iu.dsc.tws.examples.basic.comms.batch;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.batch.BReduce;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.basic.comms.BenchWorker;
import edu.iu.dsc.tws.examples.basic.comms.DataGenerator;

public class BReduceExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BReduceExample.class.getName());

  private BReduce reduce;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
        jobParameters.getTaskStages());

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;

    // create the communication
    reduce = new BReduce(communicator, taskPlan, sources, target,
        new IdentityFunction(), new FinalReduceReceiver(), MessageType.INTEGER);


    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(id, taskPlan,
        jobParameters.getTaskStages(), 0);
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    reduce.progress();
  }

  private class MapWorker implements Runnable {
    private int task;

    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker: " + id);
      int[] data = DataGenerator.generateIntData(jobParameters.getSize());
      for (int i = 0; i < jobParameters.getIterations(); i++) {
        // lets generate a message
        int flag = 0;
        if (i == jobParameters.getIterations() - 1) {
          flag = MessageFlags.FLAGS_LAST;
        }

        while (!reduce.reduce(task, data, flag)) {
          // lets wait a litte and try again
          reduce.progress();
        }
      }
      LOG.info(String.format("%d Done sending", id));
    }
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
  protected boolean isDone() {
    return false;
  }

  public class FinalReduceReceiver implements ReduceReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      return true;
    }
  }

  public class IdentityFunction implements ReduceFunction {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      count++;
      if (count % 100 == 0) {
        LOG.info(String.format("Partial received %d", count));
      }
      return t1;
    }
  }
}
