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

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.config.DataPartitionType;

/**
 * This class can provide a config like a map where taskindex is paired with the number of
 * data points per partition. This config can be passed to Twister2 Data Partitioning functions
 * and also this can be returned to user so that the files can be read in a single pass by avoiding
 * two pass.
 */
public class DataPartitioner implements IDataPartitionFunction {

  private static final Logger LOG = Logger.getLogger(DataPartitioner.class.getName());

  private final HashMap<Integer, Integer> dataPartitionMap = new HashMap<>();

  /**
   * Number of data samples
   */
  private int samples;

  /**
   * parallelism used in the application
   */
  private int parallelism;

  /**
   * partition id to which the extra load is put on
   * this is a value in range [0,parallelism)
   */
  private int imbalancePartitionId = -1;

  /**
   * Data Partitioning method
   * It is a predefined enums
   */
  private DataPartitionType dataPartitionType;

  public DataPartitioner() {
  }

  public DataPartitioner(int sm, int par) {
    this.samples = sm;
    this.parallelism = par;
  }

  public DataPartitioner withSamples(int sm) {
    this.samples = sm;
    return this;
  }

  public DataPartitioner withParallelism(int par) {
    this.parallelism = par;
    return this;
  }

  public DataPartitioner withPartitionType(DataPartitionType partitionType) {
    this.dataPartitionType = partitionType;
    return this;
  }

  public DataPartitioner withImbalancePartitionId(int imbalanceId) {
    this.imbalancePartitionId = imbalanceId;
    return this;
  }


  public DataPartitioner partition() {
    getDataDistribution(this.parallelism, this.samples, this.dataPartitionType);
    return this;
  }

  @Override
  public HashMap<Integer, Integer> getDataDistribution(int parallelism, int samples,
                                                       DataPartitionType dataPartitionType) {
    checkData(parallelism, samples);
    if (dataPartitionType.equals(DataPartitionType.EQUI_LOAD)) {
      doEquiLoad();
    }
    if (dataPartitionType.equals(DataPartitionType.WEIGHTED_LOAD)) {
      doWeightedLoad();
    }
    if (dataPartitionType.equals(DataPartitionType.DEFAULT)) {
      doDefaultLoad();
    }
    return dataPartitionMap;
  }

  public void doEquiLoad() {
    int numOfEquiSizePointsPerPartition = samples / parallelism;
    int remainder = samples - (numOfEquiSizePointsPerPartition * parallelism);
    for (int i = 0; i < this.parallelism; i++) {
      this.dataPartitionMap.put(i, numOfEquiSizePointsPerPartition);
    }
  }

  public void doWeightedLoad() {
    //TODO can be used for edge device related data partition
    // take the weights as a map of parallelism, number of data points
  }

  public void doDefaultLoad() {
    int numOfEquiSizePointsPerPartition = this.samples / this.parallelism;
    int remainder = this.samples - (numOfEquiSizePointsPerPartition * this.parallelism);
    if (this.imbalancePartitionId == -1) {
      this.imbalancePartitionId = 0;
    }
    this.dataPartitionMap.put(this.imbalancePartitionId,
        numOfEquiSizePointsPerPartition + remainder);
    for (int i = 0; i < this.parallelism; i++) {
      if (i == this.imbalancePartitionId) {
        continue;
      }
      this.dataPartitionMap.put(i, numOfEquiSizePointsPerPartition);
    }
  }

  public void checkData(int parallelism, int samples) {
    if (samples <= parallelism) {
      LOG.severe(String.format("Warning: Too small dataset"));
    }
    if (samples == 0 || parallelism == 0) {
      throw new RuntimeException("Samples Size or Parallelism is invalid!");
    }
  }

  public HashMap<Integer, Integer> getDataPartitionMap() {
    return this.dataPartitionMap;
  }
}
