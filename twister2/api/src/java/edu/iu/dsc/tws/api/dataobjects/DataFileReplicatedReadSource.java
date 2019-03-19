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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class is responsible for directly reading the data points from the respective filesystem
 * and add the datapoints into the DataObject.
 */
public class DataFileReplicatedReadSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataFileReplicatedReadSource.class.getName());

  private static final long serialVersionUID = -1L;

  private String fileDirectory;
  private int dimension;
  private int datasize;
  private String fileSystem = null;

  private FileSystem fs = null;
  private FSDataInputStream fdis;
  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  public DataFileReplicatedReadSource(String edgename) {
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


  public DataFileReplicatedReadSource() {
  }

  /**
   * The execute method uses the DataFileReader utils class in the data package to
   * read the input data points from the respective file system.
   */
  @Override
  public void execute() {
    try {
      Path path = new Path(fileDirectory);
      final FileStatus pathFile;
      fs = path.getFileSystem(config);
      if ("hdfs".equals(fileSystem)) {
        pathFile = fs.getFileStatus(path);
        this.fdis = fs.open(pathFile.getPath());
      } else {
        for (FileStatus file : fs.listFiles(path)) {
          this.fdis = fs.open(file.getPath());
        }
      }
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.fdis));
      Object line;
      while ((line = bufferedReader.readLine()) != null) {
        context.write(getEdgeName(), line);
      }
      bufferedReader.close();
      fdis.close();
    } catch (IOException ioe) {
      throw new RuntimeException("IOException Occured", ioe);
    }
    context.end(getEdgeName());
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    fileSystem = cfg.getStringValue(DataObjectConstants.ARGS_FILE_SYSTEM);
    fileDirectory = cfg.getStringValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DIMENSIONS));
    datasize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_CSIZE));
  }
}
