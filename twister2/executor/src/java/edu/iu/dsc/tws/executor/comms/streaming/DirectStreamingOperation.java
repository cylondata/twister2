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

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.stream.SDirect;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class DirectStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionStreamingOperation.class.getName());

  protected SDirect op;

  public DirectStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                     Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                                     DataType dataType, String edgeName) {
    super(config, network, tPlan);
    this.edgeGenerator = e;

    if (srcs.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dests == null) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    op = new SDirect(channel, taskPlan, new ArrayList<Integer>(srcs), new ArrayList<Integer>(dests),
        Utils.dataTypeToMessageType(dataType), new DirectReceiver());
    communicationEdge = e.generate(edgeName);
  }

  public boolean send(int source, IMessage message, int flags) {
    return op.partition(source, message.getContent(), flags);
  }

  public class DirectReceiver implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> targets) {

    }

    @Override
    public boolean receive(int target, Object object) {
      BlockingQueue<IMessage> messages = outMessages.get(target);

      TaskMessage msg = new TaskMessage(object,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return messages.offer(msg);
    }
  }

  public boolean progress() {
    return op.progress();
  }

  @Override
  public void close() {
    op.close();
  }
}
