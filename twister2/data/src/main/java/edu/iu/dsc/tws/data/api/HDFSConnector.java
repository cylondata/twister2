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
package edu.iu.dsc.tws.data.api;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;

/**
 * This is the abstraction class for the Hadoop Distributed File System and establishing
 * connections with HDFS connections.
 */
public class HDFSConnector implements IHDFSConnector {

  private static final Logger LOG = Logger.getLogger(HDFSConnector.class.getName());
  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private Config config;
  private String fileName;

  public HDFSConnector(Config cfg) {
    this.config = cfg;
  }

  public HDFSConnector(Config cfg, String fName) {
    this.config = cfg;
    this.fileName = fName;
  }

  /**
   * This method retrieve the HadoopFileSystem object using the Hadoop File System class.
   * @return hadoopFileSystem
   */
  @Override
  public HadoopFileSystem HDFSConnect() {
    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    HadoopFileSystem hadoopFileSystem = null;
    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return hadoopFileSystem;
  }

  public Path getPath() {
    String directoryString = HdfsDataContext.getHdfsUrlDefault(this.config) + this.fileName;
    Path path = new Path(directoryString);
    return path;
  }

  /**
   * This method retrieve the HadoopFileSystem object using the Hadoop File System class and
   * write the message using the Hadoop Output Stream class.
   * @param message
   * @param count
   */
  public void HDFSConnect(String message, int count) {

    HadoopFileSystem hadoopFileSystem = HDFSConnect();
    HadoopDataOutputStream hadoopDataOutputStream = null;
    try {
      String directoryString =
          HdfsDataContext.getHdfsUrlDefault(config) + "/user/kgovind/" + this.fileName;
      Path path = new Path(directoryString);
      if (!hadoopFileSystem.exists(path)) {
        hadoopDataOutputStream = hadoopFileSystem.create(path);
        hadoopDataOutputStream.write(
            "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
      } else if (hadoopFileSystem.exists(path)) {
        hadoopDataOutputStream = hadoopFileSystem.append(path);
        hadoopDataOutputStream.write(
            "Hello, I am appending to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          LOG.info("I am closing the streams");
          if (hadoopDataOutputStream != null) {
            hadoopDataOutputStream.close();
          }
          hadoopFileSystem.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  /**
   * This method retrieve the HadoopFileSystem object using the Hadoop File System class
   * and write the message using the Hadoop Output Stream class.
   * @param message
   */
  @Override
  public void HDFSConnect(String message) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    HadoopDataOutputStream hadoopDataOutputStream = null;
    HadoopFileSystem hadoopFileSystem = null;

    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
      String directoryString = HdfsDataContext.getHdfsUrlDefault(config) + "/" + fileName;
      Path path = new Path(directoryString);
      if (!hadoopFileSystem.exists(path)) {
        hadoopDataOutputStream = hadoopFileSystem.create(path);
        hadoopDataOutputStream.write(message.getBytes(DEFAULT_CHARSET));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
          if (hadoopDataOutputStream != null) {
            hadoopDataOutputStream.close();
          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  /**
   * This method is used to locate the datanode location of the input file name.
   * @param fName
   * @return datanodeName
   */
  public String getDFSCK(String[] fName) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));

    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    StringBuilder stringBuilder = new StringBuilder();

    String datanodeName = null;

    try {
      InetSocketAddress namenodeAddress =
          new InetSocketAddress(HdfsDataContext.getHdfsNamenodeDefault(config),
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
      out.println();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return datanodeName;
  }

  public int getLengthOfFile(String fName) {
    int length = 0;
    String inputFileName = HdfsDataContext.getHdfsDataDirectory(config) + "/" + fName;
    String directoryString = HdfsDataContext.getHdfsUrlDefault(config) + inputFileName;
    Path path = new Path(directoryString);
    HadoopFileSystem hadoopFileSystem = HDFSConnect();
    try {
      if (hadoopFileSystem.exists(path)) {
        BufferedReader br = new BufferedReader(new InputStreamReader(
            hadoopFileSystem.open(path)));
        while ((br.readLine()) != null) {
          length++;
        }
      }
    } catch (Exception ee) {
      ee.printStackTrace();
    }
    return length;
  }
}
