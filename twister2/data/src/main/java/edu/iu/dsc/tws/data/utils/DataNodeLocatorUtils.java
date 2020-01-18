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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.ToolRunner;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.IDataNodeLocatorUtils;

/**
 * This class is to find out the location of the datanodes corresponding to the input file names.
 * It retrieve the location of the input file(s) using the HDFSUtils class.
 */
public class DataNodeLocatorUtils implements IDataNodeLocatorUtils {

  private static final Logger LOG = Logger.getLogger(DataNodeLocatorUtils.class.getName());

  private Config config;
  private String datasetName;

  public DataNodeLocatorUtils(Config cfg) {
    this.config = cfg;
  }

  /**
   * This method receives the input data list for each vertex and find the location of the
   * datanodes in the HDFS and returns the data node list.
   */
  public List<String> findDataNodesLocation(List<String> inputFileList) {

    List<String> dataNodes = new ArrayList<>();
    FileSystem fileSystem;
    try {
      for (String anInputFileList : inputFileList) {
        Path path = new Path(anInputFileList);
        fileSystem = FileSystemUtils.get(path.toUri(), config);
        this.datasetName = anInputFileList;
        if (config.get(DataObjectConstants.FILE_SYSTEM).equals(Context.TWISTER2_HDFS_FILESYSTEM)) {
          FileStatus fileStatus = fileSystem.getFileStatus(new Path(datasetName));
          if (!fileStatus.getPath().isNullOrEmpty()) {
            //dataNodes = getDataNodes(new String[]{this.datasetName});
            dataNodes = getDataNodes();
          }
        } else if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
            Context.TWISTER2_LOCAL_FILESYSTEM)) {
          FileStatus fileStatus = fileSystem.getFileStatus(new Path(datasetName));
          if (!fileStatus.getPath().isNullOrEmpty()) {
            String datanodeName = InetAddress.getLocalHost().getHostName();
            dataNodes.add(datanodeName);
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException("IOException Occured");
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

    List<String> dataNodes = new ArrayList<>();
    FileSystem fileSystem;
    try {
      Path path = new Path(inputFileName);
      fileSystem = FileSystemUtils.get(path.toUri(), config);
      if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
          Context.TWISTER2_HDFS_FILESYSTEM)) {
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (!fileStatus.getPath().isNullOrEmpty()) {
          dataNodes = getDataNodes();
        }
      } else if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
          Context.TWISTER2_LOCAL_FILESYSTEM)) {
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (!fileStatus.getPath().isNullOrEmpty()) {
          String datanodeName = InetAddress.getLocalHost().getHostName();
          dataNodes.add(datanodeName);
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException("IOException Occured");
    }
    return dataNodes;
  }

  /**
   * This method retrieve all the datanodes of a hdfs cluster
   */
  private List<String> getDataNodes() throws IOException {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));

    List<String> datanodesList = new ArrayList<>();
    InetSocketAddress namenodeAddress = new InetSocketAddress(
        HdfsDataContext.getHdfsNamenodeDefault(config),
        HdfsDataContext.getHdfsNamenodePortDefault(config));
    DFSClient dfsClient = new DFSClient(namenodeAddress, conf);
    ClientProtocol nameNode = dfsClient.getNamenode();
    DatanodeInfo[] datanodeReport =
        nameNode.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
    for (DatanodeInfo di : datanodeReport) {
      datanodesList.add(di.getHostName());
    }
    return datanodesList;
  }

  /**
   * This method retrieve the datanode name of the file in the hdfs cluster
   */
  private List<String> getDataNodes(String[] fName) throws IOException {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);

    List<String> datanodesList = new ArrayList<>();
    InetSocketAddress namenodeAddress = new InetSocketAddress(
        HdfsDataContext.getHdfsNamenodeDefault(config),
        HdfsDataContext.getHdfsNamenodePortDefault(config));
    DFSClient dfsClient = new DFSClient(namenodeAddress, conf);
    ClientProtocol nameNode = dfsClient.getNamenode();
    DatanodeInfo[] datanodeReport =
        nameNode.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
    for (DatanodeInfo di : datanodeReport) {
      datanodesList.add(di.getHostName());
    }

    //To retrieve the datanode name of the respective file
    try {
      ToolRunner.run(new DFSck(conf, out), fName);
      out.println();
    } catch (Exception e) {
      throw new RuntimeException("Exception Occured:" + e.getMessage());
    }
    bStream.close();
    out.close();
    return datanodesList;
  }
}
