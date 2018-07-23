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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;


public class GatherBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(GatherBatchOperation.class.getName());
  private DataFlowGather op;

  public GatherBatchOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e,
                      DataType dataType, String edgeName, Config config, TaskPlan taskPlan) {
    this.edge = e;
    communicationEdge = e.generate(edgeName);
    op = new DataFlowGather(channel, srcs, dest, new FinalGatherReceiver(), 0, 0,
        config, Utils.dataTypeToMessageType(dataType), taskPlan, e.getIntegerMapping(edgeName));
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
    LOG.info("===CommunicationEdge : " + communicationEdge);
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e, DataType dataType,
                      DataType keyType, String edgeName, Config config, TaskPlan taskPlan) {
    op = new DataFlowGather(channel, srcs, dest, new FinalGatherReceiver(), 0, 0,
        config, Utils.dataTypeToMessageType(dataType), Utils.dataTypeToMessageType(keyType),
        taskPlan, e.getIntegerMapping(edgeName));
    communicationEdge = e.generate(edgeName);
    LOG.info("===CommunicationEdge : " + communicationEdge);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.info("Message : " + message.getContent());
    return op.send(source, message.getContent(), flags);
  }

  @Override
  public void send(int source, IMessage message, int dest, int flags) {
    //LOG.info("Message : " + message.getContent());
    op.send(source, message.getContent(), flags, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }


  private class FinalGatherReceiver implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {

      // add the object to the map
      if (object instanceof List) {
        for (Object o : (List) object) {
          TaskMessage msg = new TaskMessage(o,
              edge.getStringMapping(communicationEdge), target);
          outMessages.get(target).offer(msg);
          //    LOG.info("Source : " + source + ", Message : " + msg.getContent() + ", Target : "
          //        + target + ", Destination : " + destination);

        }
      }
      return true;

    }

    public void progress() {

    }
  }

}
