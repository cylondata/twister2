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

import edu.iu.dsc.tws.common.config.Config;

public class KMeansJobParameters {

  private static final Logger LOG = Logger.getLogger(KMeansJobParameters.class.getName());

  /**
   * Number of iterations
   */
  private int iterations;

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Dimension of the datapoints and centroids
   */
  private int dimension;

  /**
   * Number of clusters
   */
  private int clusters;

  /**
   * Number of datapoints to be generated
   */
  private int numberOfPoints;

  /**
   * Datapoints file name
   */
  private String pointsFile;

  /**
   * Centroid file name
   */
  private String centersFile;

  /**
   * Filesytem to store the generated data "local" or "hdfs"
   */
  private String fileSystem;

  /**
   * Seed value for the data points random number generation
   */
  private int pointsSeedValue;

  /**
   * Seed value for the centroids random number generation
   */
  private int centroidsSeedValue;

  /**
   * Data input value represents data to be generated or else to read it directly from
   * the input file and the options may be "generate" or "read"
   */
  private String dataInput;

  /**
   * File Name
   */
  private String fileName;

  private String dataType;

  public int getIterations() {
    return iterations;
  }

  public int getWorkers() {
    return workers;
  }

  public String getFileName() {
    return fileName;
  }

  public String getDataType() {
    return dataType;
  }

  public int getDimension() {
    return dimension;
  }

  public int getClusters() {
    return clusters;
  }

  public String getPointsFile() {
    return pointsFile;
  }

  public String getCentersFile() {
    return centersFile;
  }

  public String getFileSystem() {
    return fileSystem;
  }

  public int getPointsSeedValue() {
    return pointsSeedValue;
  }

  public int getCentroidsSeedValue() {
    return centroidsSeedValue;
  }

  public String getDataInput() {
    return dataInput;
  }

  public int getNumberOfPoints() {
    return numberOfPoints;
  }

  public KMeansJobParameters(int iterations, int workers) {
    this.iterations = iterations;
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   * @param cfg
   * @return
   */
  public static KMeansJobParameters build(Config cfg) {

    String fileName = cfg.getStringValue(KMeansConstants.ARGS_FNAME.trim());
    String pointFile = cfg.getStringValue(KMeansConstants.ARGS_POINTS.trim());
    String centerFile = cfg.getStringValue(KMeansConstants.ARGS_CENTERS.trim());
    String fileSys = cfg.getStringValue(KMeansConstants.ARGS_FILESYSTEM.trim());
    String dataInput = cfg.getStringValue(KMeansConstants.ARGS_DATA_INPUT.trim());

    int workers = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_WORKERS));
    int iterations = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_ITR));
    int dim = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));
    int points = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_NUMBER_OF_POINTS));

    int numberOfClusters = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_CLUSTERS));
    int pointsVal = Integer.parseInt(cfg.getStringValue(
        KMeansConstants.ARGS_POINTS_SEED_VALUE));
    int centroidsVal = Integer.parseInt(cfg.getStringValue(
        KMeansConstants.ARGS_CENTERS_SEED_VALUE));

    KMeansJobParameters jobParameters = new KMeansJobParameters(iterations, workers);

    jobParameters.workers = workers;
    jobParameters.iterations = iterations;
    jobParameters.dimension = dim;
    jobParameters.numberOfPoints = points;

    jobParameters.fileName = fileName;
    jobParameters.pointsFile = pointFile;
    jobParameters.centersFile = centerFile;
    jobParameters.fileSystem = fileSys;
    jobParameters.dataInput = dataInput;

    jobParameters.clusters = numberOfClusters;
    jobParameters.pointsSeedValue = pointsVal;
    jobParameters.centroidsSeedValue = centroidsVal;

    return jobParameters;
  }

  @Override
  public String toString() {

    LOG.fine("workers:" + workers + "\titeration:" + iterations + "\tdimension:" + dimension
        + "\tnumber of clusters:" + clusters + "\tfilename:" + fileName
        + "\tdatapoints file:" + pointsFile + "\tcenters file:" + centersFile
        + "\tfilesys:" + fileSystem);
    return "JobParameters{"
        + ", iterations=" + iterations
        + ", workers=" + workers
        + '}';
  }
}

