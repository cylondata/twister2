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
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.stream.SGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class GatherStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(GatherStreamingOperation.class.getName());
  private SGather op;

  public GatherStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                  Set<Integer> srcs, int dest, EdgeGenerator e,
                                  DataType dataType, String edgeName, TaskPlan taskPlan) {
    super(config, network, tPlan);

    if (srcs.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    this.edgeGenerator = e;
    communicationEdge = e.generate(edgeName);
    op = new SGather(channel, taskPlan, srcs, dest, Utils.dataTypeToMessageType(dataType),
        new GatherRcvr());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.info("Message : " + message.getContent());
    return op.gather(source, message.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress();
  }


  private class GatherRcvr implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> targets) {

    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {

      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }
  }

  @Override
  public void close() {
    op.close();
  }
}
