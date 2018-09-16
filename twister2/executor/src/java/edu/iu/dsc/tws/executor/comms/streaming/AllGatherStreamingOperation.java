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
package edu.iu.dsc.tws.executor.comms.streaming;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.stream.SAllGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class AllGatherStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllGatherStreamingOperation.class.getName());

  protected SAllGather op;

  public AllGatherStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                     Set<Integer> sources, Set<Integer>  dest, EdgeGenerator e,
                                     DataType dataType, String edgeName) {
    super(config, network, tPlan);

    if (sources.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dest.size() == 0) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    this.edgeGenerator = e;
    op = new SAllGather(channel, taskPlan, sources, dest,
        new FinalReduceReceive(), Utils.dataTypeToMessageType(dataType));
    communicationEdge = e.generate(edgeName);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.gather(source, message.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress();
  }

  private class FinalReduceReceive implements BulkReceiver {
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void init(Config cfg, Set<Integer> targets) {

    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        if (messages.offer(msg)) {
          return true;
        }
      }
      return true;
    }
  }
}
