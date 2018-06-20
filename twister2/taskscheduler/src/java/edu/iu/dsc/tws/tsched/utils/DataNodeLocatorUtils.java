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
package edu.iu.dsc.tws.tsched.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;

/**
 * This class is just to retrieve the data node location values corresponding to the
 * dataset file name. Later, it will be overcome by the actual values from the
 * data management part.
 */
public class DataNodeLocatorUtils {

  private static final Logger LOG = Logger.getLogger(DataNodeLocatorUtils.class.getName());

  private String datasetName;
  private List<String> inputDataList = new ArrayList<String>();
  private Config config;

  public DataNodeLocatorUtils(Config config1) {
    this.config = config1;
  }

  /**
   * This method receives the input data list for each vertex and find the location
   * of the datanodes in the HDFS file system and returns the data node list for
   * each vertex.
   * @param inputDataList1
   * @return
   */
  public List<String> findDataNodesLocation(List<String> inputDataList1) {

    this.inputDataList = inputDataList1;
    List<String> dataNodes = new ArrayList<>();

    Configuration conf = new Configuration(false);
    org.apache.hadoop.fs.FileSystem hadoopFileSystem = null;
    HadoopFileSystem hadoopFileSystem1;

    for (int i = 0; i < this.inputDataList.size(); i++) {
      this.datasetName = this.inputDataList.get(i);
      conf.addResource(new org.apache.hadoop.fs.Path(
          HdfsDataContext.getHdfsConfigDirectory(config)));
      try {
        hadoopFileSystem1 =
            new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
        Path path = new Path(datasetName);
        FileStatus fileStatus = hadoopFileSystem1.getFileStatus(path);
        if (!fileStatus.getPath().isNullOrEmpty()) {
          String fileURL = fileStatus.getPath().toString();
          /*StringTokenizer stringTokenizer = new StringTokenizer(fileURL, ":");
          while (stringTokenizer.hasMoreTokens()) {
            String key = stringTokenizer.nextToken();
            System.out.println("String tokenizer:" + key.toString());
          }*/
          LOG.info("HDFS File URL is:" + fileURL);
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
    }
    return dataNodes;
  }

  /**
   * This method receives the input file name of a vertex and find the location
   * of the datanodes in the HDFS file system and returns the data node list of the
   * vertex.
   * @param inputFileName
   * @return
   */
  public List<String> findDataNodesLocation(String inputFileName) {

    this.datasetName = inputFileName;
    List<String> dataNodes = new ArrayList<>();

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(
        HdfsDataContext.getHdfsConfigDirectory(config)));

    org.apache.hadoop.fs.FileSystem hadoopFileSystem = null;
    HadoopFileSystem hadoopFileSystem1;
    try {
      hadoopFileSystem1 =
          new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
      Path path = new Path(datasetName);
      FileStatus fileStatus = hadoopFileSystem1.getFileStatus(path);
      if (!fileStatus.getPath().isNullOrEmpty()) {
        String fileURL = fileStatus.getPath().toString();
        LOG.info("HDFS File URL is:" + fileURL);
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


  //To assign a different hostnames for the datasets...
 /* public List<String> findDataNodes() {
    List<String> dataNodes = new ArrayList<>();
    if ("dataset1.txt".equals(datasetName)) {
      dataNodes.add("datanode1");
      dataNodes.add("datanode2");
    } else if ("dataset2.txt".equals(datasetName)) {
      dataNodes.add("datanode3");
      dataNodes.add("datanode4");
    } else if ("dataset3.txt".equals(datasetName)) {
      dataNodes.add("datanode5");
      dataNodes.add("datanode6");
    } else if ("dataset4.txt".equals(datasetName)) {
      dataNodes.add("datanode7");
      dataNodes.add("datanode8");
    }
    return dataNodes;
  }*/
}

