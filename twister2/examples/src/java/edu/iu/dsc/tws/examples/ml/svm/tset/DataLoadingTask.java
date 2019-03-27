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

import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalFixedInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;

public class DataLoadingTask extends BaseSource<double[][]> {

  private static final Logger LOG = Logger.getLogger(DataLoadingTask.class.getName());

  private BinaryBatchModel binaryBatchModel;

  private SVMJobParameters svmJobParameters;

  private boolean read = false;

  private int dataSize;

  private int dimension;

  private int parallelism;

  private Config config;

  private double[][] localPoints; //store non-replicated data in memory

  private DataSource<double[][], InputSplit<double[][]>> source;

  private String dataType = "train";

  private boolean debug = false;

  public DataLoadingTask(BinaryBatchModel binaryBatchModel) {
    this.binaryBatchModel = binaryBatchModel;
  }

  public DataLoadingTask(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
  }

  public DataLoadingTask(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters,
                         String dataType) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
    this.dataType = dataType;
  }

  @Override
  public void prepare() {
    this.config = context.getConfig();
    this.parallelism = context.getParallelism();
    // dimension is +1 features as the input data comes along with the label
    this.dimension = this.binaryBatchModel.getFeatures() + 1;
    if ("train".equalsIgnoreCase(this.dataType)) {
      this.dataSize = this.binaryBatchModel.getSamples();
      this.localPoints = new double[this.dataSize / (this.parallelism + 1)][this.dimension];
      this.source = new DataSource(config, new LocalFixedInputPartitioner(new
          Path(this.svmJobParameters.getTrainingDataDir()), this.parallelism, config,
          this.dataSize), this.parallelism);
    }
    if ("test".equalsIgnoreCase(this.dataType)) {
      this.dataSize = this.svmJobParameters.getTestingSamples();
      this.localPoints = new double[this.dataSize / (this.parallelism + 1)][this.dimension];
      this.source = new DataSource(config, new LocalFixedInputPartitioner(new
          Path(this.svmJobParameters.getTestingDataDir()), this.parallelism, config,
          this.dataSize), this.parallelism);
    }
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
  public double[][] next() {
    LOG.fine("Context Prepare Center Task Index:" + context.getIndex());
    InputSplit inputSplit = this.source.getNextSplit(context.getIndex());
    int totalCount = 0;
    while (inputSplit != null) {
      try {
        int count = 0;
        while (!inputSplit.reachedEnd()) {
          String value = (String) inputSplit.nextRecord(null);
          if (value == null) {
            break;
          }
          String[] splts = value.split(",");
          if (debug) {
            LOG.info(String.format("Count %d , splits %d, dimensions %d", count, splts.length,
                this.dimension));
          }

          if (count >= this.localPoints.length) {
            break; // TODO : unbalance division temp fix
          }
          for (int i = 0; i < this.dimension; i++) {
            this.localPoints[count][i] = Double.valueOf(splts[i]);
          }
          if (value != null) {
            count += 1;
          }
        }
        inputSplit = this.source.getNextSplit(context.getIndex());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to read the input", e);
      }
    }
    return this.localPoints;
  }

}

