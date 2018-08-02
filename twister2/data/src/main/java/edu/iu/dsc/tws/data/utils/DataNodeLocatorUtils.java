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
import edu.iu.dsc.tws.data.api.HDFSConnector;
import edu.iu.dsc.tws.data.api.IDataNodeLocatorUtils;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;

/**
 * This class is to retrieve the datanode names values corresponding to the
 * dataset filenames.
 */
public class DataNodeLocatorUtils implements IDataNodeLocatorUtils {

  private static final Logger LOG = Logger.getLogger(DataNodeLocatorUtils.class.getName());

  private Config config;
  private HDFSConnector hdfsConnector;
  private String datasetName;
  private List<String> inputDataList = new ArrayList<>();

  public DataNodeLocatorUtils(Config config1) {
    this.config = config1;
  }

  //TODO: It could be modified to get all the vertexes of the task graph and
  //TODO: return the map which stores the vertex name -> datanodes list.

  /**
   * This method receives the input data list for each vertex and find the location
   * of the datanodes in the HDFS and returns the data node list.
   *
   * @param inputDataList1
   * @return datanodeList
   */
  public List<String> findDataNodesLocation(List<String> inputDataList1) {

    HadoopFileSystem hadoopFileSystem = null;

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(
        HdfsDataContext.getHdfsConfigDirectory(config)));

    hdfsConnector = new HDFSConnector(config);
    this.inputDataList = inputDataList1;

    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    List<String> dataNodes = new ArrayList<>();
    for (int i = 0; i < this.inputDataList.size(); i++) {
      this.datasetName = this.inputDataList.get(i);

      String[] fName = new String[0];
      if (this.inputDataList.size() == 1) {
        fName = new String[this.inputDataList.size()];
        fName[0] = this.inputDataList.get(i);
      } else if (this.inputDataList.size() > 1) {
        fName = new String[this.inputDataList.size() - 1];
        fName[0] = this.inputDataList.get(i);
      }

      try {
        Path path = new Path(datasetName);
        FileStatus fileStatus = hadoopFileSystem.getFileStatus(path);
        if (!fileStatus.getPath().isNullOrEmpty()) {
          String fileURL = fileStatus.getPath().toString();
          String datanodeName = hdfsConnector.getDFSCK(fName);
          LOG.fine("HDFS URL:" + fileURL + "\tDataNode:"
              + datanodeName + "\tDataSet:" + datasetName);
          dataNodes.add(datanodeName); //proper data node
          dataNodes.add("samepledatenode1"); //this is just for testing (it will be removed)
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return dataNodes;
  }

  /**
   * This method receives the input file name of a vertex and find the location
   * of the datanodes in the HDFS and returns the data node list.
   *
   * @param inputFileName
   * @return datanodeList
   */
  public List<String> findDataNodesLocation(String inputFileName) {

    HadoopFileSystem hadoopFileSystem = null;

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(
        HdfsDataContext.getHdfsConfigDirectory(config)));

    hdfsConnector = new HDFSConnector(config);

    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    List<String> dataNodes = new ArrayList<>();
    String[] fName = new String[0];
    fName[0] = datasetName;

    try {
      Path path = new Path(datasetName);
      FileStatus fileStatus = hadoopFileSystem.getFileStatus(path);
      if (!fileStatus.getPath().isNullOrEmpty()) {
        String fileURL = fileStatus.getPath().toString();
        String datanodeName = hdfsConnector.getDFSCK(fName);
        LOG.fine("HDFS URL:" + fileURL + "\tDataNode:"
            + datanodeName + "\tDataSet:" + datasetName);
        if ("dataset1.txt".equals(datasetName)) {
          dataNodes.add("datanode1");
          dataNodes.add("datanode2");
        } else if ("dataset2.txt".equals(datasetName)) {
          dataNodes.add("datanode1");
          dataNodes.add("datanode2");
        } else if ("dataset3.txt".equals(datasetName)) {
          dataNodes.add("datanode1");
          dataNodes.add("datanode2");
        } else if ("dataset4.txt".equals(datasetName)) {
          dataNodes.add("datanode1");
          dataNodes.add("datanode2");
        }
      }
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }
    return dataNodes;
  }
}

