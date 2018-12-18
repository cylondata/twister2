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
package edu.iu.dsc.tws.api.htgjob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskConfigurations;
import edu.iu.dsc.tws.common.config.Config;

public final class Twister2MetagraphBuilder {

  private static final Logger LOG = Logger.getLogger(Twister2MetagraphBuilder.class.getName());

  private ConnectionMode connectionMode = ConnectionMode.BROADCAST;
  private int defaultParallelism;
  private String htgName;

  private Map<String, Twister2Metagraph.SubGraph> subGraphsMap = new HashMap<>();

  private List<Twister2MetagraphConnection> metagraphComputeConnections = new ArrayList<>();
  private List<Twister2MetagraphConnection> metagraphSourceConnections = new ArrayList<>();

  public static Twister2MetagraphBuilder newBuilder(Config cfg) {
    return new Twister2MetagraphBuilder(cfg);
  }

  private Twister2MetagraphBuilder(Config cfg) {
    this.defaultParallelism = TaskConfigurations.getDefaultParallelism(cfg, 1);
  }

  public String getHtgName() {
    return htgName;
  }

  public void setHtgName(String htgName) {
    this.htgName = htgName;
  }

  public ConnectionMode getConnectionMode() {
    return connectionMode;
  }

  public void setConnectionMode(ConnectionMode mode) {
    this.connectionMode = mode;
  }

  public Twister2MetagraphConnection addSource(String name, Config config) {

    //Assign the config value to the graphs.
    Twister2Metagraph.SubGraph subGraph = new Twister2Metagraph.SubGraph(
        name, 2.0, 512, 1.0, 2, 1, config);
    subGraphsMap.put(name, subGraph);
    return createTwister2MetagraphConnection(name);
  }

  private Twister2MetagraphConnection createTwister2MetagraphConnection(String name) {
    Twister2MetagraphConnection sc = new Twister2MetagraphConnection(name);
    metagraphComputeConnections.add(sc);
    return sc;
  }

  public Twister2MetagraphConnection addSink(String name, Config config) {

    //Assign the config value to the graphs.
    Twister2Metagraph.SubGraph subGraph = new Twister2Metagraph.SubGraph(name,
        2.0, 1024, 1.0, 2, 1, config);
    subGraphsMap.put(name, subGraph);
    return createTwister2MetagraphConnection(name);
  }

  public Twister2Metagraph build() {

    Twister2Metagraph twister2HTGMetaGraph = new Twister2Metagraph();
    twister2HTGMetaGraph.setConnectionMode(connectionMode);
    twister2HTGMetaGraph.setHTGName(htgName);

    for (Map.Entry<String, Twister2Metagraph.SubGraph> e : subGraphsMap.entrySet()) {
      twister2HTGMetaGraph.addSubGraph(e.getKey(), e.getValue());
      LOG.fine("Key and Value:" + e.getKey() + "\t" + e.getValue());
    }

    for (Twister2MetagraphConnection c : metagraphComputeConnections) {
      c.buildCompute(twister2HTGMetaGraph);
    }

    for (Twister2MetagraphConnection c : metagraphSourceConnections) {
      c.buildSource(twister2HTGMetaGraph);
    }

    return twister2HTGMetaGraph;
  }
}


