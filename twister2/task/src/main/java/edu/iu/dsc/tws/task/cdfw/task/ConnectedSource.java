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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.task.impl.TaskConfigurations;

/**
 * Connected source
 */
public class ConnectedSource extends BaseSource implements Receptor {
  private DataObject<?> dSet;

  private String edge = TaskConfigurations.DEFAULT_EDGE;

  private boolean finished = false;

  private DataPartition<?> data;

  private DataPartitionConsumer<?> iterator;

  public ConnectedSource() {
  }

  public ConnectedSource(String edge) {
    this.edge = edge;
  }

  @Override
  public void execute() {
    if (finished) {
      return;
    }

    if (data == null) {
      data = dSet.getPartitions(context.taskIndex());
      iterator = data.getConsumer();
    }

    if (iterator.hasNext()) {
      context.write(edge, iterator.next());
    } else {
      context.end(edge);
      finished = true;
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }

  public String getEdge() {
    return edge;
  }

  public void setEdge(String edge) {
    this.edge = edge;
  }

  @Override
  public void add(String name, DataObject<?> dataObject) {
    dSet = dataObject;
  }
}
