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
package edu.iu.dsc.tws.api.cdfw.task;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskConfigurations;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DSet;
import edu.iu.dsc.tws.dataset.PSet;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * Connected source
 */
public class ConnectedSource extends BaseSource implements Receptor {
  private PSet<Object> pSet;

  private TaskContext context;

  private String edge = TaskConfigurations.DEFAULT_EDGE;

  private boolean finished = false;

  public ConnectedSource() {
  }

  public ConnectedSource(String edge) {
    this.edge = edge;
  }

  @Override
  public void add(String name, DSet<Object> data) {
    pSet = data.getPartitions(context.getWorkerId(), context.taskIndex());
  }

  @Override
  public void execute() {
    if (finished) {
      return;
    }

    if (pSet.hasNext()) {
      context.write(edge, pSet.next());
    } else {
      context.end(edge);
      finished = true;
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
  }

  public String getEdge() {
    return edge;
  }

  public void setEdge(String edge) {
    this.edge = edge;
  }
}
