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
package edu.iu.dsc.tws.graphapi.impl;

import java.util.HashMap;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.graphapi.api.BasicComputation;
import edu.iu.dsc.tws.graphapi.vertex.DefaultVertex;
import edu.iu.dsc.tws.graphapi.vertex.VertexStatus;

public class TestForPagerank extends BasicComputation {

  private GraphPartiton graphPartiton = new DataPartitionPageRank(Context.TWISTER2_DIRECT_EDGE,
      parallelism, graphsize);
  private GraphInitialization graphInitialization = new DataIniPageRank(
      Context.TWISTER2_DIRECT_EDGE,
      parallelism, graphsize);

  private SourceTask sourceTask = new SourceTaskPageRank();
  private ComputeTask computeTask = new ComputeTaskPageRank();



  @Override
  public ComputeGraph computation() {
    return buildComputationTG(parallelism, config, sourceTask, computeTask,
        null, MessageTypes.DOUBLE_ARRAY);
  }

  @Override
  public ComputeGraph graphInitialization() {
    return buildGraphInitialaizationTG(dataDirectory, graphsize, parallelism, config,
        graphInitialization);
  }

  @Override
  public ComputeGraph graphpartition() {
    return buildGraphPartitionTG(dataDirectory, graphsize, parallelism, config,
        graphPartiton);
  }


  class DataPartitionPageRank extends GraphPartiton {

    DataPartitionPageRank(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
    }

    @Override
    public void constructDataStr(IMessage message) {

    }
  }

  class DataIniPageRank extends GraphInitialization {

    DataIniPageRank(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
    }

    @Override
    public void constructDataStr(HashMap<String, VertexStatus> hashMap, IMessage message) {

    }
  }

  private class SourceTaskPageRank extends SourceTask {

    @Override
    public void sendmessage(HashMap<String, DefaultVertex> hashMap1,
                            HashMap<String, VertexStatus> hashMap2) {

    }
  }

  private class ComputeTaskPageRank extends ComputeTask {

    @Override
    public void calculation(Iterator iterator) {

    }
  }
}
