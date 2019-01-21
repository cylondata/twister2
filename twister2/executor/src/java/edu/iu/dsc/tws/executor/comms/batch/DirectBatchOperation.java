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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BDirect;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class DirectBatchOperation extends AbstractParallelOperation {
  private BDirect op;

  public DirectBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                                 Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                                 DataType dataType, String edgeName) {
    super(config, network, tPlan);
    this.edgeGenerator = e;

    // we assume a uniform task id association, so this will work
    ArrayList<Integer> sources = new ArrayList<>(srcs);
    Collections.sort(sources);
    ArrayList<Integer> targets = new ArrayList<>(dests);
    Collections.sort(targets);

    op = new BDirect(channel, taskPlan, sources, targets,
        new PartitionReceiver(), Utils.dataTypeToMessageType(dataType));
    communicationEdge = e.generate(edgeName);
  }

  public void send(int source, IMessage message) {
    op.direct(source, message.getContent(), 0);
  }

  public boolean send(int source, IMessage message, int dest) {
    return op.direct(source, message.getContent(), 0);
  }

  public class PartitionReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  @Override
  public void close() {
    op.close();
  }
}
