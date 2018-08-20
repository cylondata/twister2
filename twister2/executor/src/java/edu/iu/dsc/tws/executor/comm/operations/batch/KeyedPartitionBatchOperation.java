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
package edu.iu.dsc.tws.executor.comm.operations.batch;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.SimpleKeyBasedPartitionSelector;
import edu.iu.dsc.tws.comms.op.batch.BKeyedPartition;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;

public class KeyedPartitionBatchOperation extends AbstractParallelOperation {

  private static final Logger LOG = Logger.getLogger(KeyedPartitionBatchOperation.class.getName());

  protected BKeyedPartition keyedPartition;
  private Communicator communicator;
  private TaskPlan taskPlan;

  public KeyedPartitionBatchOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
    this.communicator = new Communicator(config, network);
    this.taskPlan = tPlan;
  }

  public void prepare(Set<Integer> sources, Set<Integer> dests, EdgeGenerator e,
                      MessageType dataType, MessageType keyType,  String edgeName) {
    this.keyedPartition = new BKeyedPartition(communicator, taskPlan, sources, dests,
        dataType, keyType, new PartitionBatchReceiver(), new SimpleKeyBasedPartitionSelector());

  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    Object object = message.getContent();
    // TODO : from IMessage a retrieving method must be written to get key and message
    Object keyObject = null;
    Object messagageObject = null;
    return keyedPartition.partition(source, keyObject, messagageObject, flags);
  }

  @Override
  public boolean progress() {
    return keyedPartition.progress();
  }

  public boolean hasPending() {
    return true;
  }

  public class PartitionBatchReceiver implements BatchReceiver {
    private int count = 0;
    private int expected;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public void receive(int target, Iterator<Object> it) {

    }
  }


}
