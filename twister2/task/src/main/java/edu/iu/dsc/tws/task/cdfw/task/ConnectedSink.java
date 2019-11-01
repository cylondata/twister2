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
package edu.iu.dsc.tws.task.cdfw.task;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.CollectionPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;

public class ConnectedSink extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(ConnectedSink.class.getName());

  private String inputKey;

  private CollectionPartition<Object> partition;

  public ConnectedSink() {
  }

  public ConnectedSink(String inputkey) {
    this.inputKey = inputkey;
  }

  @Override
  public boolean execute(IMessage message) {
    LOG.info("context task index:" + context.taskIndex() + "execute message:" + message);
    partition = new CollectionPartition<>(context.taskIndex());
    if (message.getContent() instanceof Iterator) {
      Iterator<Object> itr = (Iterator<Object>) message.getContent();
      while (itr.hasNext()) {
        partition.add(itr.next());
      }
    } else {
      partition.add(message.getContent());
    }
    LOG.info("context task index:" + context.taskIndex()
        + "Partition Size:" + partition.getConsumer().next());
    return true;
  }

  @Override
  public DataPartition<Object> get() {
    return new EntityPartition<>(partition);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }

  @Override
  public IONames getCollectibleNames() {
    return IONames.declare(inputKey);
  }
}
