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
package edu.iu.dsc.tws.data.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.IDataNodeLocatorUtils;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;

/**
 * This class is to find out the location of the datanodes corresponding to the input file names.
 * It retrieve the location of the input file(s) using the HDFSUtils class.
 */
public class DataNodeLocatorUtils implements IDataNodeLocatorUtils {

  private static final Logger LOG = Logger.getLogger(DataNodeLocatorUtils.class.getName());

  private Config config;
  private HdfsUtils hdfsUtils;
  private String datasetName;

  public DataNodeLocatorUtils(Config cfg) {
    this.config = cfg;
  }

  /**
   * This method receives the input data list for each vertex and find the location of the
   * datanodes in the HDFS and returns the data node list.
   */
  public List<String> findDataNodesLocation(List<String> inputFileList) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    hdfsUtils = new HdfsUtils(config);
    HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();

    List<String> dataNodes = new ArrayList<>();
    for (int i = 0; i < inputFileList.size(); i++) {
      this.datasetName = inputFileList.get(i);
      String[] fName;
      if (inputFileList.size() == 1) {
        fName = new String[1];
        fName[0] = inputFileList.get(i);
      } else {
        fName = new String[inputFileList.size() - 1];
        fName[0] = inputFileList.get(i);
      }

      try {
        Path path = new Path(datasetName);
        FileStatus fileStatus = hadoopFileSystem.getFileStatus(path);
        if (!fileStatus.getPath().isNullOrEmpty()) {
          String fileURL = fileStatus.getPath().toString();
          String datanodeName = hdfsUtils.getDFSCK(fName);
          dataNodes.add(datanodeName);
          LOG.fine("HDFS URL:" + fileURL + "\tDataNode:" + datanodeName);
        }
      } catch (IOException ioe) {
        throw new RuntimeException("IO Exception occured:", ioe);
      } catch (NullPointerException npe) {
        throw new RuntimeException("Datanode not able to retrieve:", npe);
      }
    }
    return dataNodes;
  }

  /**
   * This method receives the input file name of a vertex and find the location of the datanodes
   * in the HDFS and returns the data node list.
   *
   * @return datanodes list
   */
  public List<String> findDataNodesLocation(String inputFileName) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    hdfsUtils = new HdfsUtils(config);
    HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();

    List<String> dataNodes = new ArrayList<>();
    try {
      Path path = new Path(inputFileName);
      FileStatus fileStatus = hadoopFileSystem.getFileStatus(path);
      if (!fileStatus.getPath().isNullOrEmpty()) {
        String fileURL = fileStatus.getPath().toString();
        String[] fName = {inputFileName};
        String datanodeName = hdfsUtils.getDFSCK(fName);
        LOG.fine("HDFS URL:" + fileURL + "\tDataNode:" + datanodeName);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("IO Exception occured:", ioe);
    }
    return dataNodes;
  }
}

