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
package edu.iu.dsc.tws.examples.ml.svm.integration.test;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.typed.AbstractIterableDataCompute;

public class IterativeSVMWeightVectorObjectCompute1 extends AbstractIterableDataCompute<String> {

  private static final Logger LOG = Logger.getLogger(IterativeSVMWeightVectorObjectCompute1.class
      .getName());

  private static final long serialVersionUID = -254264120110286748L;

  private static final String DELIMITER = ",";

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
  private double[] dataPointsLocal;

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
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

  public IterativeSVMWeightVectorObjectCompute1(String edgeName, int parallelism, int datasize,
                                                int features) {
    this.edgeName = edgeName;
    this.parallelism = parallelism;
    this.datasize = datasize;
    this.features = features;
    this.dataPointsLocal = new double[this.features];
  }

  public IterativeSVMWeightVectorObjectCompute1(String edgeName, int datasize, int features) {
    this.edgeName = edgeName;
    this.datasize = datasize;
    this.features = features;
    this.dataPointsLocal = new double[this.features];
  }


  @Override
  public boolean execute(IMessage<Iterator<String>> message) {
    if (message.getContent().hasNext()) {
      String s = message.getContent().next();
      if (s != null) {
        this.dataPointsLocal = DataUtils.arrayFromString(s, DELIMITER);
        context.writeEnd(getEdgeName(), this.dataPointsLocal);
      } else {
        LOG.severe(String.format("Null datapoint received"));
      }
    }
    return true;
  }
}
