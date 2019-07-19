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
package edu.iu.dsc.tws.examples.ml.svm.tset;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.LocalTextInputPartitioner1;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;

public class WeightVectorLoad extends BaseSourceFunc<double[]> {

  private static final long serialVersionUID = -8333645489977825619L;

  private static final Logger LOG = Logger.getLogger(WeightVectorLoad.class.getName());

  private static final String DELIMITER = ",";

  private BinaryBatchModel binaryBatchModel;

  private SVMJobParameters svmJobParameters;

  private boolean read = false;

  private int dataSize;

  private int dimension;

  private int parallelism;

  private Config config;

  private double[] localPoints; //store non-replicated data in memory

  private DataSource<double[], InputSplit<double[]>> source;

  private String dataType = "weightVector";

  private boolean debug = false;

  public WeightVectorLoad(BinaryBatchModel binaryBatchModel) {
    this.binaryBatchModel = binaryBatchModel;
  }

  public WeightVectorLoad(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);

    this.config = context.getConfig();
    this.parallelism = context.getParallelism();
    LOG.info(String.format("%d, %d, %d", this.getTSetContext().getIndex(),
        this.svmJobParameters.getParallelism(), context.getParallelism()));
    this.dimension = this.binaryBatchModel.getFeatures();
    this.localPoints = new double[this.dimension];
    this.source = new DataSource<double[], InputSplit<double[]>>(config,
        new LocalTextInputPartitioner1(new Path(this.svmJobParameters.getWeightVectorDataDir()),
            this.parallelism, config), this.parallelism);

  }

  @Override
  public boolean hasNext() {
    if (!read) {
      read = true;
      return true;
    }
    return false;
  }

  @Override
  public double[] next() {
    traverseFile();
    return this.localPoints;
  }

  private void traverseFile() {
    InputSplit inputSplit = this.source.getNextSplit(getTSetContext().getIndex());
    while (inputSplit != null) {
      try {
        int count = 0;
        while (!inputSplit.reachedEnd()) {
          String value = (String) inputSplit.nextRecord(null);
          if (value == null) {
            break; //TODO : continue or break
          }
          this.localPoints = DataUtils.arrayFromString(value, DELIMITER, true);
          count++;
        }
        inputSplit = this.source.getNextSplit(getTSetContext().getIndex());
      } catch (IOException ex) {
        LOG.log(Level.SEVERE, "Failed to read the input", ex);
      }
    }
  }

}
