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
package edu.iu.dsc.tws.data.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;

/**
 * This is the util class for creating the Hadoop Distributed File System, getting the path,
 * length of the file, and locate the datanode location of the input files.
 */
public class HdfsUtils {

  private static final Logger LOG = Logger.getLogger(HdfsUtils.class.getName());

  private Config config;
  private String fileName;

  public HdfsUtils(Config cfg) {
    this.config = cfg;
  }

  public HdfsUtils(Config cfg, String fName) {
    this.config = cfg;
    this.fileName = fName;
  }

  /**
   * This method create and return the file system respective to the configuration file.
   *
   * @return hadoopFileSystem
   */
  public HadoopFileSystem createHDFSFileSystem() {
    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    conf.set("fs.defaultFS", HdfsDataContext.getHdfsUrlDefault(config));
    HadoopFileSystem hadoopFileSystem;
    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
    } catch (IOException ioe) {
      throw new RuntimeException("Hadoop File System Creation Error:", ioe);
    }
    return hadoopFileSystem;
  }

  public Path getPath() {
    String directoryString = HdfsDataContext.getHdfsUrlDefault(this.config) + "/" + this.fileName;
    Path path = new Path(directoryString);
    return path;
  }


  /**
   * This method is used to locate the datanode location of the input file name.
   *
   * @return datanodeName
   */
  public String getDFSCK(String[] fName) throws IOException {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);

    String datanodeName = null;
    InetSocketAddress namenodeAddress = new InetSocketAddress(
        HdfsDataContext.getHdfsNamenodeDefault(config),
        HdfsDataContext.getHdfsNamenodePortDefault(config));
    DFSClient dfsClient = new DFSClient(namenodeAddress, conf);
    ClientProtocol nameNode = dfsClient.getNamenode();
    DatanodeInfo[] datanodeReport =
        nameNode.getDatanodeReport(HdfsConstants.DatanodeReportType.ALL);
    for (DatanodeInfo di : datanodeReport) {
      datanodeName = di.getHostName();
    }
    //It will be enabled later...!
    //run(new DFSck(conf, out), fName);
    //out.println();
    return datanodeName;
  }

  /**
   * To get the length of the filename.
   */
  public int getLengthOfFile(String fName) {
    int length = 0;
    Path path = new Path(fName);
    HadoopFileSystem hadoopFileSystem = createHDFSFileSystem();
    try {
      if (hadoopFileSystem.exists(path)) {
        BufferedReader br = new BufferedReader(new InputStreamReader(
            hadoopFileSystem.open(path)));
        while ((br.readLine()) != null) {
          length++;
        }
      }
    } catch (IOException ee) {
      throw new RuntimeException("Reading Exception:", ee);
    }
    return length;
  }
}
