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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
 * This is the abstraction class for the HDFS file system and establishing
 * HDFS file system connections.
 */
public class HDFSConnector implements IHDFSConnector {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final Logger LOG = Logger.getLogger(HDFSConnector.class.getName());
  private Config config;
  private String outputFile;
  private FileHandler fileHandler;

  public HDFSConnector(Config config1) {
    this.config = config1;
  }

  public HDFSConnector(Config config1, String outputFile1) {
    this.config = config1;
    this.outputFile = outputFile1;
  }


  @Override
  public HadoopFileSystem HDFSConnect() {
    try {
      fileHandler = new FileHandler("/home/kgovind/twister2/twister2hdfs.log");
      LOG.addHandler(fileHandler);
      SimpleFormatter simpleFormatter = new SimpleFormatter();
      fileHandler.setFormatter(simpleFormatter);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
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

  /**
   * To connect the HDFS file system
   */
  public void HDFSConnect(String message, int count) {

    HadoopFileSystem hadoopFileSystem = HDFSConnect();
    HadoopDataOutputStream hadoopDataOutputStream = null;

    try {
      String directoryString =
          HdfsDataContext.getHdfsUrlDefault(config) + "/user/kannan/" + outputFile;
      Path path = new Path(directoryString);
      if (!hadoopFileSystem.exists(path)) {
        /*LOG.info("Directory String Is:%%%" + directoryString + "\t"
            + "Output File Is:%%%" + outputFile);*/
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

  @Override
  public void HDFSConnect(String message) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
    HadoopDataOutputStream hadoopDataOutputStream = null;
    HadoopFileSystem hadoopFileSystem = null;

    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
      String directoryString = HdfsDataContext.getHdfsUrlDefault(config)
          + "/user/kannan/" + outputFile;

      /*LOG.info("Directory String Is:%%%" + directoryString + "\t"
          + "Output File Is:%%%" + outputFile);*/

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
   * This method will be used to locate the datanode location of the input file.
   */
  public String getDFSCK(String[] fName) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));

    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);

    String datanodeName = null;
    StringBuilder stringBuilder = new StringBuilder();

    try {
      InetSocketAddress namenodeAddress =
          new InetSocketAddress("hairy.soic.indiana.edu", 9000);
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
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return datanodeName;
  }
}
