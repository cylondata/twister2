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
package edu.iu.dsc.tws.examples.batch.mds;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;

public final class MDSWorkerParameters {

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

  private MDSWorkerParameters(int workers) {
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static MDSWorkerParameters build(Config cfg) {

    int workers = Integer.parseInt(cfg.getStringValue(DataObjectConstants.WORKERS));
    int parallelismVal = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.PARALLELISM_VALUE));

    int dsize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
    int dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));

    String datapointDirectory = cfg.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
    String fileSystem = cfg.getStringValue(DataObjectConstants.FILE_SYSTEM);
    String byteType = cfg.getStringValue(DataObjectConstants.BYTE_TYPE);

    MDSWorkerParameters jobParameters = new MDSWorkerParameters(workers);

    jobParameters.workers = workers;
    jobParameters.parallelismValue = parallelismVal;

    jobParameters.dsize = dsize;
    jobParameters.dimension = dimension;

    jobParameters.datapointDirectory = datapointDirectory;
    jobParameters.filesystem = fileSystem;
    jobParameters.byteType = byteType;

    return jobParameters;
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

  public String getDatapointDirectory() {
    return datapointDirectory;
  }

  public int getDimension() {
    return dimension;
  }

  public int getParallelismValue() {
    return parallelismValue;
  }

  public String getByteType() {
    return byteType;
  }

  @Override
  public String toString() {

    return "JobParameters{"
        + ", workers=" + workers
        + '}';
  }
}
