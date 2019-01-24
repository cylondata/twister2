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

package org.apache.storm.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.twister2.Twister2Bolt;
import org.apache.storm.topology.twister2.Twister2Spout;

import edu.iu.dsc.tws.api.task.SourceConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.task.api.INode;

public class TopologyBuilder {

  private TaskGraphBuilder taskGraphBuilder;
  private HashMap<String, INode> nodes = new HashMap<>();

  private Set<String> sinkNodes = new HashSet<>();//these are the sinks in twitser2
  private Set<String> sourceNodes = new HashSet<>();//these are the sources in twitser2
  private Set<String> computeNodes = new HashSet<>();//these are the computes in twister2

  public TopologyBuilder() {
    this.taskGraphBuilder = TaskGraphBuilder.newBuilder(null);
  }

  public StormTopology createTopology() {
    this.sourceNodes.forEach(source -> {
      Twister2Spout twister2Spout = (Twister2Spout) nodes.get(source);
      this.taskGraphBuilder.addSource(
          source,
          twister2Spout,
          twister2Spout.getParallelism()
      );
    });

    this.computeNodes.forEach(compute -> {
      Twister2Bolt twister2Bolt = (Twister2Bolt) nodes.get(compute);
      this.taskGraphBuilder.addCompute(
          compute,
          twister2Bolt,
          twister2Bolt.getParallelism()
      );
    });

    return null;
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, 1);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    Twister2Bolt twister2Bolt = new Twister2Bolt(bolt, source -> {
      //the source is not a sink anymore, it is a source
      this.sinkNodes.remove(source);
      this.computeNodes.add(source);
    });
    twister2Bolt.setParallelism(parallelismHint.intValue());
    this.nodes.put(id, twister2Bolt);
    this.sinkNodes.add(id);//add all to the sink nodes initially
    return twister2Bolt.getBoltDeclarer();
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, 1);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    throw new UnsupportedOperationException(
        "This operation is not supported in twitser2 yet"
    );
  }

  public BoltDeclarer setBolt(String id, IWindowedBolt bolt) throws IllegalArgumentException {
    return setBolt(id, bolt, 1);
  }

  public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelismHint) throws
      IllegalArgumentException {
    throw new UnsupportedOperationException(
        "This operation is not supported in twitser2 yet"
    );
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, 1);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    Twister2Spout twister2Spout = new Twister2Spout(spout);
    twister2Spout.setParallelism(parallelismHint.intValue());
    this.nodes.put(id, twister2Spout);
    this.sourceNodes.add(id);
    return twister2Spout.getSpoutDeclarer();
  }
}
