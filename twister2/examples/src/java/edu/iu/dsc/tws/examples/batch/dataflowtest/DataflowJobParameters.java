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
package edu.iu.dsc.tws.examples.batch.dataflowtest;

import edu.iu.dsc.tws.api.dataobjects.DataObjectConstants;
import edu.iu.dsc.tws.common.config.Config;

public final class DataflowJobParameters {

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Number of datapoints to be generated
   */
  private int dsize;

  /**
   * Dimension of the datapoints and centroids
   */
  private int dimension;

  /**
   * Number of iterations
   */
  private int iterations;

  /**
   * Task parallelism value
   */
  private int parallelismValue;

  /**
   * Number of clusters
   */
  private int numberOfClusters;

  /**
   * Represents file system "local" or "hdfs"
   */
  private String filesystem;

  protected DataflowJobParameters() {
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static DataflowJobParameters build(Config cfg) {

    int workers = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DSIZE));
    int parallelismVal = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.ARGS_PARALLELISM_VALUE));
    int iterations = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.ARGS_ITERATIONS));

    DataflowJobParameters jobParameters = new DataflowJobParameters();

    jobParameters.workers = workers;
    jobParameters.iterations = iterations;
    jobParameters.dsize = dsize;
    jobParameters.parallelismValue = parallelismVal;

    return jobParameters;
  }

  public int getNumberOfClusters() {
    return numberOfClusters;
  }

  public String getFilesystem() {
    return filesystem;
  }

  public int getWorkers() {
    return workers;
  }

  public int getDsize() {
    return dsize;
  }

  public int getDimension() {
    return dimension;
  }

  public int getIterations() {
    return iterations;
  }

  public int getParallelismValue() {
    return parallelismValue;
  }

  @Override
  public String toString() {
    return "JobParameters{"
        + ", workers=" + workers
        + '}';
  }
}
