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
package edu.iu.dsc.tws.api.dataobjects;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.utils.DataFileReader;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class is responsible for directly reading the data points from the respective filesystem
 * and add the datapoints into the DataObject.
 */
public class DataFileReadSource extends BaseSource {

  private static final long serialVersionUID = -1L;

  private String fileDirectory;
  private int dimension;
  private int datasize;

  private DataFileReader fileReader;
  private double[][] datapoints = null;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  public DataFileReadSource(String edgename) {
    this.edgeName = edgename;
  }

  /**
   * Getter property to set the edge name
   */
  public String getEdgeName() {
    return edgeName;
  }

  /**
   * Setter property to set the edge name
   */
  public void setEdgeName(String edgeName) {
    this.edgeName = edgeName;
  }


  public DataFileReadSource() {
  }

  /**
   * The execute method uses the DataFileReader utils class in the data package to
   * read the input data points from the respective file system.
   */
  @Override
  public void execute() {
    datapoints = fileReader.readData(new Path(fileDirectory), dimension, datasize);
    context.writeEnd(getEdgeName(), datapoints);
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    String fileSystem = cfg.getStringValue(DataObjectConstants.ARGS_FILE_SYSTEM);
    fileReader = new DataFileReader(config, fileSystem);
    fileDirectory = cfg.getStringValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DIMENSIONS));
    datasize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_CSIZE));
  }
}
