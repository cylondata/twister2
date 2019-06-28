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
package edu.iu.dsc.tws.examples.stockanalysis;

import edu.iu.dsc.tws.common.config.Config;

public final class StockAnalysisWorkerParameters {

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Number of datapoints to be generated
   */
  private int dsize;

  /**
   * Datapoints directory
   */
  private String datapointDirectory;

  /**
   * Dimension of the datapoints and centroids
   */
  private int dimension;

  /**
   * Task parallelism value
   */
  private int parallelismValue;

  /**
   * Represents file system "local" or "hdfs"
   */
  private String filesystem;

  /**
   * Represents "big" or "little" endian
   */
  private String byteType;

  /**
   * Config file for MDS
   */
  private String configFile;
  /**
   * Datapoints to be generated or to read
   */
  private String dataInput;
  private String dinputFile;
  private String outputDirectory;
  private String numberOfDays;
  private String startDate;
  private String endDate;
  private String mode;
  private String distanceType;

  private StockAnalysisWorkerParameters(int workerId) {
    this.workers = workerId;
  }

  /**
   * build the constants
   * @param cfg
   * @return
   */
  public static StockAnalysisWorkerParameters build(Config cfg) {

    int workers = Integer.parseInt(cfg.getStringValue(StockAnalysisConstants.WORKERS));
    int parallelismVal = Integer.parseInt(cfg.getStringValue(
        StockAnalysisConstants.PARALLELISM_VALUE));
    int dsize = Integer.parseInt(cfg.getStringValue(StockAnalysisConstants.DSIZE));
    int dimension = Integer.parseInt(cfg.getStringValue(StockAnalysisConstants.DIMENSIONS));

    String datapointDirectory = cfg.getStringValue(StockAnalysisConstants.DINPUT_DIRECTORY);
    String fileSystem = cfg.getStringValue(StockAnalysisConstants.FILE_SYSTEM);
    String byteType = cfg.getStringValue(StockAnalysisConstants.BYTE_TYPE);
    String configFile = cfg.getStringValue(StockAnalysisConstants.CONFIG_FILE);
    String dataInput = cfg.getStringValue(StockAnalysisConstants.DATA_INPUT);

    String inputfile = cfg.getStringValue(StockAnalysisConstants.DINPUT_FILE);
    String outputdirectory = cfg.getStringValue(StockAnalysisConstants.DOUTPUT_DIRECTORY);
    String days = cfg.getStringValue(StockAnalysisConstants.NUMBER_OF_DAYS);
    String mode = cfg.getStringValue(StockAnalysisConstants.MODE);
    String startdate = cfg.getStringValue(StockAnalysisConstants.START_DATE);
    String enddate = cfg.getStringValue(StockAnalysisConstants.END_DATE);
    String distancetype = cfg.getStringValue(StockAnalysisConstants.DISTANCE_TYPE);

    StockAnalysisWorkerParameters jobParameters = new StockAnalysisWorkerParameters(workers);
    jobParameters.workers = workers;
    jobParameters.parallelismValue = parallelismVal;
    jobParameters.dsize = dsize;
    jobParameters.dimension = dimension;

    jobParameters.datapointDirectory = datapointDirectory;
    jobParameters.filesystem = fileSystem;
    jobParameters.byteType = byteType;
    jobParameters.configFile = configFile;
    jobParameters.dataInput = dataInput;

    jobParameters.dinputFile = inputfile;
    jobParameters.outputDirectory = outputdirectory;
    jobParameters.mode = mode;
    jobParameters.startDate = startdate;
    jobParameters.endDate = enddate;
    jobParameters.numberOfDays = days;
    jobParameters.distanceType = distancetype;
    return jobParameters;
  }

  public String getDistanceType() {
    return distanceType;
  }

  public int getWorkers() {
    return workers;
  }

  public int getDsize() {
    return dsize;
  }

  public String getDatapointDirectory() {
    return datapointDirectory;
  }

  public int getDimension() {
    return dimension;
  }

  public int getParallelismValue() {
    return parallelismValue;
  }

  public String getFilesystem() {
    return filesystem;
  }

  public String getByteType() {
    return byteType;
  }

  public String getConfigFile() {
    return configFile;
  }

  public String getDataInput() {
    return dataInput;
  }

  public String getDinputFile() {
    return dinputFile;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public String getNumberOfDays() {
    return numberOfDays;
  }

  public String getStartDate() {
    return startDate;
  }

  public String getEndDate() {
    return endDate;
  }

  public String getMode() {
    return mode;
  }

  @Override
  public String toString() {
    return "JobParameters{" + ", workers=" + workers + '}';
  }
}
