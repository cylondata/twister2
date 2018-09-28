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
package edu.iu.dsc.tws.executor.comms.batch;

import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class GatherBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(GatherBatchOperation.class.getName());
  private BGather op;

  public GatherBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                              Set<Integer> srcs, int dest, EdgeGenerator e,
                              DataType dataType, String edgeName, boolean shuffle) {
    super(config, network, tPlan);
    this.edgeGenerator = e;
    communicationEdge = e.generate(edgeName);
    op = new BGather(channel, taskPlan, srcs, dest, Utils.dataTypeToMessageType(dataType),
        new FinalGatherReceiver(), shuffle);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.info("Message : " + message.getContent());
    return op.gather(source, message.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  public boolean hasPending() {
    return op.hasPending();
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  private class FinalGatherReceiver implements BulkReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      // add the object to the map
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }
  }

}
