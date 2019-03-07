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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataObjectConstants;
import edu.iu.dsc.tws.common.config.Config;

public final class KMeansJobParameters {

  private static final Logger LOG = Logger.getLogger(KMeansJobParameters.class.getName());

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Number of datapoints to be generated
   */
  private int dsize;

  /**
   * Number of cluster points to be generated
   */
  private int csize;

  /**
   * Datapoints directory
   */
  private String datapointDirectory;

  /**
   * Centroid directory
   */
  private String centroidDirectory;

  /**
   * Output directory
   */
  private String outputDirectory;

  /**
   * Number of files
   */
  private int numFiles;

  /**
   * type of file system "shared" or "local"
   */
  private boolean shared;

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

  private KMeansJobParameters(int workers) {
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static KMeansJobParameters build(Config cfg) {

    String datapointDirectory = cfg.getStringValue(DataObjectConstants.ARGS_DINPUT_DIRECTORY);
    String centroidDirectory = cfg.getStringValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    String outputDirectory = cfg.getStringValue(DataObjectConstants.ARGS_OUTPUT_DIRECTORY);
    String fileSystem = cfg.getStringValue(DataObjectConstants.ARGS_FILE_SYSTEM);

    int workers = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_CSIZE));
    int dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DIMENSIONS));
    int parallelismVal = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.ARGS_PARALLELISM_VALUE));
    int iterations = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.ARGS_ITERATIONS));
    int numFiles = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_NUMBER_OF_FILES));
    int numberOfClusters = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.ARGS_NUMBER_OF_CLUSTERS));
    boolean shared = cfg.getBooleanValue(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM);

    KMeansJobParameters jobParameters = new KMeansJobParameters(workers);

    jobParameters.workers = workers;
    jobParameters.dimension = dimension;
    jobParameters.centroidDirectory = centroidDirectory;
    jobParameters.datapointDirectory = datapointDirectory;
    jobParameters.outputDirectory = outputDirectory;
    jobParameters.numFiles = numFiles;
    jobParameters.iterations = iterations;
    jobParameters.dsize = dsize;
    jobParameters.csize = csize;
    jobParameters.shared = shared;
    jobParameters.parallelismValue = parallelismVal;
    jobParameters.filesystem = fileSystem;
    jobParameters.numberOfClusters = numberOfClusters;

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

  public int getCsize() {
    return csize;
  }

  public String getDatapointDirectory() {
    return datapointDirectory;
  }

  public String getCentroidDirectory() {
    return centroidDirectory;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public int getNumFiles() {
    return numFiles;
  }

  public boolean isShared() {
    return shared;
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
