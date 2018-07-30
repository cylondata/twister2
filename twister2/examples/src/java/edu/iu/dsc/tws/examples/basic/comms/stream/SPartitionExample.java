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
package edu.iu.dsc.tws.examples.basic.comms.stream;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.LoadBalanceDestinationSelector;
import edu.iu.dsc.tws.comms.op.stream.SPartition;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.basic.comms.BenchWorker;

public class SPartitionExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SPartitionExample.class.getName());

  private SPartition partition;
  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
        jobParameters.getTaskStages());

    Set<Integer> sources = new HashSet<>();
    Set<Integer> targets = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    // create the communication
    partition = new SPartition(communicator, taskPlan, sources, targets,
        MessageType.INTEGER, new BCastReceiver(), new LoadBalanceDestinationSelector());

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
    partition.progress();
  }

  @Override
  protected boolean isDone() {
    return false;
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    return false;
  }

  public class BCastReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      LOG.log(Level.INFO, "Received message");
      return false;
    }

    @Override
    public void progress() {

    }
  }
}
