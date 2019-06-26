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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;

public class IterativeSVMPrimaryWeightVectorObjectCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(IterativeSVMPrimaryDataObjectDirectSink.class
      .getName());

  private static final long serialVersionUID = -254264120110286748L;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  /**
   * Task parallelism
   */
  private int parallelism;

  /**
   * Data size
   */
  private int datasize;

  /**
   * Dimension of the datapoints
   */
  private int features;

  /**
   * Datapoints array
   */
  private double[][] dataPointsLocal;

  public IterativeSVMPrimaryWeightVectorObjectCompute(String edgeName, int parallelism,
                                                      int datasize,
                                                      int features) {
    this.edgeName = edgeName;
    this.parallelism = parallelism;
    this.datasize = datasize;
    this.features = features;
  }

  public IterativeSVMPrimaryWeightVectorObjectCompute(String edgeName, int datasize, int features) {
    this.edgeName = edgeName;
    this.datasize = datasize;
    this.features = features;
  }

  public String getEdgeName() {
    return edgeName;
  }

  public void setEdgeName(String edgeName) {
    this.edgeName = edgeName;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getDatasize() {
    return datasize;
  }

  public void setDatasize(int datasize) {
    this.datasize = datasize;
  }

  public int getFeatures() {
    return features;
  }

  public void setFeatures(int features) {
    this.features = features;
  }

  @Override
  public boolean execute(IMessage message) {
    List<String> values = new ArrayList<>();
    while (((Iterator) message.getContent()).hasNext()) {
      values.add(String.valueOf(((Iterator) message.getContent()).next()));
    }
    dataPointsLocal = new double[values.size()][this.features];
    String line;
    for (int i = 0; i < values.size(); i++) {
      line = values.get(i);
      String[] data = line.split(",");
      for (int j = 0; j < this.features; j++) {
        this.dataPointsLocal[i][j] = Double.parseDouble(data[j].trim());
      }
      context.write(getEdgeName(), this.dataPointsLocal);
    }
    context.end(getEdgeName());
    return true;
  }
}
