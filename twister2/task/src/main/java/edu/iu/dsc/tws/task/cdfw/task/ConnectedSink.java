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

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.CollectionPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;

public class ConnectedSink extends BaseCompute implements Collector {
  /**
   * The name of the data set
   */
  private String inputKey;

  /**
   * The partition to use
   */
  private CollectionPartition<Object> partition;

  public ConnectedSink() {
  }

  public ConnectedSink(String inputkey) {
    this.inputKey = inputkey;
  }

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() instanceof  Iterator) {
      while (((Iterator<Object>) message.getContent()).hasNext()) {
        partition.add(((Iterator<Object>) message.getContent()).next());
      }
    }
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    partition = new CollectionPartition<>();
  }

  @Override
  public DataPartition<Object> get() {
    return new EntityPartition<>(partition);
  }

  @Override
  public IONames getCollectibleNames() {
    return IONames.declare(inputKey);
  }
}
