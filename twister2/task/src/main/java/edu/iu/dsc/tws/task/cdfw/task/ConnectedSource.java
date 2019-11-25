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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.task.impl.TaskConfigurations;

/**
 * Connected source for the connected dataflow example
 */
public class ConnectedSource extends BaseSource implements Receptor {

  private static final Logger LOG = Logger.getLogger(ConnectedSource.class.getName());

  private String edge = TaskConfigurations.DEFAULT_EDGE;

  private DataPartition<?> dataPartition;

  private Object dataObject = null;

  private String inputKey;

  public ConnectedSource() {
  }

  public ConnectedSource(String edge) {
    this.edge = edge;
  }

  public ConnectedSource(String edge, String inputkey) {
    this.edge = edge;
    this.inputKey = inputkey;
  }

  @Override
  public void execute() {
    dataObject = dataPartition.first();
    context.writeEnd(edge, dataObject);
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
  public void add(String name, DataPartition<?> data) {
    if (inputKey.equals(name)) {
      this.dataPartition = data;
    }
  }

  @Override
  public IONames getReceivableNames() {
    return IONames.declare(inputKey);
  }
}
