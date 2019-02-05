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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.twister2.Twister2Bolt;
import org.apache.storm.topology.twister2.Twister2BoltGrouping;
import org.apache.storm.topology.twister2.Twister2Spout;
import org.apache.storm.topology.twister2.Twister2StormNode;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TopologyBuilder implements Serializable {

  private static final Logger LOG = Logger.getLogger(TopologyBuilder.class.getName());

  private transient TaskGraphBuilder taskGraphBuilder;
  private HashMap<String, Twister2StormNode> nodes = new HashMap<>();

  private Set<String> sinkNodes = new HashSet<>(); //these are the sinks in twister2
  private Set<String> sourceNodes = new HashSet<>(); //these are the sources in twister2
  private Set<String> computeNodes = new HashSet<>(); //these are the computes in twister2

  public TopologyBuilder() {
    this.taskGraphBuilder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
  }

  private String generateEdgeName(Twister2BoltGrouping t2BoltGrouping, String destination) {
    if (t2BoltGrouping.getStreamId() != null) {
      return t2BoltGrouping.getStreamId(); //todo
    }
    return String.format(
        "%s_%s", //sourceComponentID_StreamID_DestinationComponentID
        t2BoltGrouping.getComponentId(),
        t2BoltGrouping.getStreamId()
    );
  }

  private void defineGrouping(Twister2Bolt twister2Bolt,
                              ComputeConnection computeConnection) {
    String nodeId = twister2Bolt.getId();
    twister2Bolt.getBoltDeclarer().getGroupings().forEach(grouping -> {
      //setting input fields for the bolt
      twister2Bolt.addInboundFieldsForEdge(
          grouping.getStreamId(),
          nodes.get(grouping.getComponentId())
              .getOutFieldsForEdge(grouping.getStreamId())
      );

      switch (grouping.getGroupingTechnique()) {
        case DIRECT:
          LOG.info("Adding direct grouping : " + grouping);
          computeConnection.direct(
              grouping.getComponentId(),
              this.generateEdgeName(grouping, nodeId),
              DataType.OBJECT
          );
          break;
        case SHUFFLE:
          LOG.info("Adding shuffle grouping : " + grouping
              + "{" + grouping.getComponentId() + ","
              + this.generateEdgeName(grouping, nodeId));
          computeConnection.partition(
              grouping.getComponentId(),
              this.generateEdgeName(grouping, nodeId),
              DataType.OBJECT
          );
          break;
        case FIELD:
          computeConnection.keyedPartition(
              grouping.getComponentId(),
              this.generateEdgeName(grouping, nodeId),
              DataType.OBJECT,
              DataType.OBJECT
          );
          nodes.get(grouping.getComponentId()).setKeyedOutEdges(
              grouping.getStreamId(),
              grouping.getGroupingKey()
          );
          break;
        case ALL:
          computeConnection.broadcast(
              grouping.getComponentId(),
              this.generateEdgeName(grouping, nodeId),
              DataType.OBJECT
          );
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported grouping technique : "
                  + grouping.getGroupingTechnique());
      }
    });
  }

  public StormTopology createTopology() {
    this.sourceNodes.forEach(source -> {
      Twister2Spout twister2Spout = (Twister2Spout) nodes.get(source);
      LOG.info("Adding source : " + source);
      this.taskGraphBuilder.addSource(
          source,
          twister2Spout,
          twister2Spout.getParallelism()
      );
    });

    this.computeNodes.forEach(compute -> {
      Twister2Bolt twister2Bolt = (Twister2Bolt) nodes.get(compute);
      ComputeConnection computeConnection = this.taskGraphBuilder.addCompute(
          compute,
          twister2Bolt,
          twister2Bolt.getParallelism()
      );
      this.defineGrouping(twister2Bolt, computeConnection);
    });

    this.sinkNodes.forEach(sink -> {
      Twister2Bolt twister2Bolt = (Twister2Bolt) nodes.get(sink);
      ComputeConnection computeConnection = this.taskGraphBuilder.addSink(
          sink,
          twister2Bolt,
          twister2Bolt.getParallelism()
      );
      this.defineGrouping(twister2Bolt, computeConnection);
    });

    this.taskGraphBuilder.setMode(OperationMode.STREAMING);

    return new StormTopology(this.taskGraphBuilder.build());
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, 1);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    return this.createT2Bolt(id, bolt, parallelismHint);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return this.setBolt(id, bolt, 1);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    return this.createT2Bolt(id, bolt, parallelismHint);
  }

  private BoltDeclarer createT2Bolt(String id, Object bolt, Number parallelismHint) {
    Twister2Bolt twister2Bolt = new Twister2Bolt(id, bolt, source -> {
      // the source is not a sink anymore,
      // it is a source for another node(compute node)
      boolean sourceWasInSinkNodes = this.sinkNodes.remove(source);
      if (sourceWasInSinkNodes) {
        this.computeNodes.add(source);
      }
    });
    twister2Bolt.setParallelism(parallelismHint.intValue());
    this.nodes.put(id, twister2Bolt);
    this.sinkNodes.add(id); //add all to the sink nodes initially
    return twister2Bolt.getBoltDeclarer();
  }

  public BoltDeclarer setBolt(String id, IWindowedBolt bolt) throws IllegalArgumentException {
    return this.setBolt(id, bolt, 1);
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
    Twister2Spout twister2Spout = new Twister2Spout(id, spout);
    twister2Spout.setParallelism(parallelismHint.intValue());
    this.nodes.put(id, twister2Spout);
    this.sourceNodes.add(id);
    return twister2Spout.getSpoutDeclarer();
  }
}
