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
package edu.iu.dsc.tws.graphapi.sssp;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;

public final class SsspParameters {

  private static final Logger LOG = Logger.getLogger(SsspParameters.class.getName());

  /**
   * Number of Workers
   */
  private int workers;

  /**
   * Number of edgelist to be generated
   */
  private int dsize;

  /**
   * source vertex for sssp
   */
  private String sourcevertex;



  /**
   * Datapoints directory
   */
  private String datapointDirectory;





  /**
   * Number of files
   */
  private int numFiles;

  /**
   * type of file system "shared" or "local"
   */
  private boolean shared;


  /**
   * Task parallelism value
   */
  private int parallelismValue;

  /**
   * Represents file system "local" or "hdfs"
   */
  private String filesystem;

  private SsspParameters(int workers) {
    this.workers = workers;
  }

  /**
   * This method is to build the job parameters which is based on the configuration value.
   */
  public static SsspParameters build(Config cfg) {

    String datapointDirectory = cfg.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
    String fileSystem = cfg.getStringValue(DataObjectConstants.FILE_SYSTEM);

    int workers = Integer.parseInt(cfg.getStringValue(DataObjectConstants.WORKERS));
    int dsize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
    int parallelismVal = Integer.parseInt(
        cfg.getStringValue(DataObjectConstants.PARALLELISM_VALUE));
    int numFiles = Integer.parseInt(cfg.getStringValue(DataObjectConstants.NUMBER_OF_FILES));
    boolean shared = cfg.getBooleanValue(DataObjectConstants.SHARED_FILE_SYSTEM);
    String sourceV = cfg.getStringValue(DataObjectConstants.SOURCE_VERTEX);

    SsspParameters jobParameters = new SsspParameters(workers);

    jobParameters.workers = workers;
    jobParameters.datapointDirectory = datapointDirectory;
    jobParameters.numFiles = numFiles;
    jobParameters.dsize = dsize;
    jobParameters.shared = shared;
    jobParameters.parallelismValue = parallelismVal;
    jobParameters.filesystem = fileSystem;
    jobParameters.sourcevertex = sourceV;

    return jobParameters;
  }

  public String getSourcevertex() {
    return sourcevertex;
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


  public int getNumFiles() {
    return numFiles;
  }

  public boolean isShared() {
    return shared;
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
