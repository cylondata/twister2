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
package edu.iu.dsc.tws.examples.batch.cdfw;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.config.Config;

/**
 * This class is responsible for handling the data objects and converting into two-dimensional array
 * of objects.
 */
public class KMeansDataObjectCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(KMeansDataObjectCompute.class.getName());
  private static final long serialVersionUID = -254264120110286748L;

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

  /**
   * Dimension of the datapoints
   */
  private int dimension;

  /**
   * Datapoints array
   */
  private double[][] dataPointsLocal;

  public KMeansDataObjectCompute() {
  }

  /**
   * The constructor to receive the edge name, data size, parallelism, dimension as an argument.
   * @param edgename
   * @param dsize
   * @param parallel
   * @param dim
   */
  public KMeansDataObjectCompute(String edgename, int dsize, int parallel, int dim) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
    this.dimension = dim;
  }

  public KMeansDataObjectCompute(String edgename, int size, int dim) {
    this.edgeName = edgename;
    this.datasize = size;
    this.dimension = dim;
  }

  public int getDatasize() {
    return datasize;
  }

  public void setDatasize(int datasize) {
    this.datasize = datasize;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
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
    List<String> values = new ArrayList<>();
    while (((Iterator) message.getContent()).hasNext()) {
      values.add(String.valueOf(((Iterator) message.getContent()).next()));
    }
    dataPointsLocal = new double[values.size()][dimension];
    String line;
    for (int i = 0; i < values.size(); i++) {
      line = values.get(i);
      String[] data = line.split(",");
      for (int j = 0; j < dimension; j++) {
        dataPointsLocal[i][j] = Double.parseDouble(data[j].trim());
      }
      context.write(getEdgeName(), dataPointsLocal);
    }
    context.end(getEdgeName());
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }
}

