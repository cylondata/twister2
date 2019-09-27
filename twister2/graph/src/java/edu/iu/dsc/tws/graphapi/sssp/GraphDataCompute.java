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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.graphapi.vertex.DefaultEdge;
import edu.iu.dsc.tws.graphapi.vertex.SsspDefaultVertex;


public class GraphDataCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(GraphDataCompute.class.getName());

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  /**
   * Task parallelism
   */
  private int parallel;

  /**
   * Data size
   */
  private int datasize;

  private String sourceVertex;


  public GraphDataCompute(String edgename, int dsize, int parallel, String sourceVertex) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
    this.sourceVertex = sourceVertex;
  }



  public GraphDataCompute(String edgename, int dsize, int parallel) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
  }

  public GraphDataCompute(String edgename, int size) {
    this.edgeName = edgename;
    this.datasize = size;
  }

  public void setSourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public String getSourceVertex() {
    return sourceVertex;
  }

  public int getDatasize() {
    return datasize;
  }

  public void setDatasize(int datasize) {
    this.datasize = datasize;
  }

  public int getParallel() {
    return parallel;
  }

  public void setParallel(int parallel) {
    this.parallel = parallel;
  }

  public String getEdgeName() {

    return edgeName;
  }

  public void setEdgeName(String edgeName) {

    this.edgeName = edgeName;
  }

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() instanceof Iterator) {
      HashMap<String, SsspDefaultVertex> hashMappartition = new HashMap<>();

      while (((Iterator) message.getContent()).hasNext()) {

        SsspDefaultVertex ssspDefaultVertex = new SsspDefaultVertex();
        ArrayList<DefaultEdge> arrayList = new ArrayList<>();
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split("\\s+");

        ssspDefaultVertex.initialize(data[0], 0, arrayList);

        ArrayList<String> adjList = new ArrayList<String>(Arrays.asList(data));
        adjList.remove(0);
        for (int i = 0; i < (adjList.size()) / 2; i++) {
          DefaultEdge defaultEdge = new DefaultEdge();
          String adjVertex = adjList.get(2 * i);
          int edgeValue = Integer.parseInt(adjList.get(2 * i + 1));
          defaultEdge.setId(adjVertex);
          defaultEdge.setValue(edgeValue);
          ssspDefaultVertex.addEdge(defaultEdge);
        }


        if (sourceVertex.equals(data[0])) {
          ssspDefaultVertex.setValue(0);
        } else {
          ssspDefaultVertex.setValue(Integer.MAX_VALUE);
        }
        hashMappartition.put(data[0], ssspDefaultVertex);
        context.write(getEdgeName(), hashMappartition);
      }
    }

    context.end(getEdgeName());
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }
}
