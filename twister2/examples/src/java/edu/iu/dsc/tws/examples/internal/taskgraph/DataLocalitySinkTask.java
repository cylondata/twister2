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
package edu.iu.dsc.tws.examples.internal.taskgraph;

import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataLocalitySinkTask extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(DataLocalitySinkTask.class.getName());
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

  public DataLocalitySinkTask(String edgename, int dsize, int parallel, int dim) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
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

  private String rValue;

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() instanceof String) {
      rValue = String.valueOf(message.getContent());
    }
    int value = 0;
    String[] data = rValue.split(",");
    dataPointsLocal = new double[data.length - 1][dimension];
    for (int i = 0; i < getDimension(); i++) {
      dataPointsLocal[value][i] = Double.parseDouble(data[i].trim());
    }
    LOG.info("Received Datapoints:" + Arrays.deepToString(dataPointsLocal));
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }


  @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), dataPointsLocal);
  }
}

