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


import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.graphapi.vertex.SsspVertexStatus;


public class SsspInitialCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(SsspInitialCompute.class.getName());

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

  /*
  Source vertex
   */

  private String sourceVertex;


  public SsspInitialCompute(String edgename, int dsize, int parallel, String sourceVertex) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
    this.sourceVertex = sourceVertex;
  }
  public SsspInitialCompute(String edgename, int dsize, int parallel) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
  }

  public SsspInitialCompute(String edgename, int size) {
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
      HashMap<String, SsspVertexStatus> hashMappartition = new HashMap<>();

      while (((Iterator) message.getContent()).hasNext()) {
        SsspVertexStatus ssspVertexStatus = new SsspVertexStatus();
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split("\\s+");
        if (data.length == 1 && data[0].equals("")) {
          continue;
        } else if (!data[0].equals("")) {
          ssspVertexStatus.setId(data[0]);
          if (sourceVertex.equals(data[0])) {
            ssspVertexStatus.setValue(0);
          } else {
            ssspVertexStatus.setValue(Integer.MAX_VALUE);
          }

          hashMappartition.put(data[0], ssspVertexStatus);
          context.write(getEdgeName(), hashMappartition);
        }
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
