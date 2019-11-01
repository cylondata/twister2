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
package edu.iu.dsc.tws.graphapi.sssp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.graphapi.vertex.SsspDefaultVertex;


public class GraphDataSink extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(GraphDataSink.class.getName());

  private static final long serialVersionUID = -1L;

  private HashMap<String, SsspDefaultVertex> dataPointsLocal;
  private String inputKey;

  public GraphDataSink(String inputkey) {
    this.inputKey = inputkey;
  }



  /**
   * This method add the received message from the DataObject Source into the data objects.
   */
  @Override
  public boolean execute(IMessage message) {
    List<HashMap<String, SsspDefaultVertex>> values = new ArrayList<>();
    while (((Iterator) message.getContent()).hasNext()) {
      values.add((HashMap<String, SsspDefaultVertex>) ((Iterator) message.getContent()).next());

    }

    for (HashMap<String, SsspDefaultVertex> value : values) {
      dataPointsLocal = value;
    }

    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }

  @Override
  public DataPartition<HashMap<String, SsspDefaultVertex>> get() {
    return new EntityPartition<>(context.taskIndex(), dataPointsLocal);
  }

  @Override
  public IONames getCollectibleNames() {
    return IONames.declare(inputKey);
  }
}
