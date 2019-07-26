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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.typed.AbstractIterableDataCompute;

public class IterativeSVMDataObjectCompute extends AbstractIterableDataCompute<String> {

  private static final Logger LOG = Logger.getLogger(IterativeSVMDataObjectCompute.class
      .getName());

  private static final long serialVersionUID = 2616064651374815799L;

  private String delemiter;

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

  public IterativeSVMDataObjectCompute(String edgeName, int parallelism, int datasize,
                                       int features, String del) {
    this.edgeName = edgeName;
    this.parallelism = parallelism;
    this.datasize = datasize;
    this.features = features;
    this.delemiter = del;
    int size = this.datasize % this.parallelism == 0
        ? (this.datasize / parallelism) + 1 : (this.datasize / parallelism);
    this.dataPointsLocal = new double[size][this.features + 1];
  }

  public IterativeSVMDataObjectCompute(String edgeName, int datasize, int features, String del) {
    this.edgeName = edgeName;
    this.datasize = datasize;
    this.features = features;
    this.delemiter = del;
    int size = this.datasize % this.parallelism == 0
        ? (this.datasize / parallelism) + 1 : (this.datasize / parallelism);
    this.dataPointsLocal = new double[size][this.features + 1];
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
  public boolean execute(IMessage<Iterator<String>> message) {
    Iterator<String> itr = message.getContent();
    int count = 0;
    while (itr.hasNext()) {
      String s = itr.next();
      if (s != null && !s.isEmpty()) {
        if (count < this.dataPointsLocal.length) {
          this.dataPointsLocal[count] = DataUtils.arrayFromString(s, delemiter, true);
        }
      } else {
        LOG.severe(String.format("Received data point is null!!!"));
      }
      count++;
    }
    context.write(getEdgeName(), this.dataPointsLocal);
    context.end(getEdgeName());
    return true;
  }
}
