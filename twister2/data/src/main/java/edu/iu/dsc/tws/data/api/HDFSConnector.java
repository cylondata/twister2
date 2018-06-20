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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

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

  public HDFSConnector(Config config1) {
    this.config = config1;
  }

  public HDFSConnector(Config config1, String outputFile1) {
    this.config = config1;
    this.outputFile = outputFile1;
  }

  /**
   * To connect the HDFS file system
   */
  public HadoopFileSystem HDFSConnect(String message, int count) {

    Configuration conf = new Configuration(false);
    conf.addResource(new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));

    HadoopDataOutputStream hadoopDataOutputStream = null;
    HadoopFileSystem hadoopFileSystem = null;
    try {
      hadoopFileSystem = new HadoopFileSystem(conf, org.apache.hadoop.fs.FileSystem.get(conf));
      String directoryString = HdfsDataContext.getHdfsUrlDefault(config)
          + "/user/kannan/" + outputFile;
      Path path = new Path(directoryString);
      if (!hadoopFileSystem.exists(path)) {
        LOG.info("Directory String Is:%%%" + directoryString + "\t"
            + "Output File Is:%%%" + outputFile);
        hadoopDataOutputStream = hadoopFileSystem.create(path);
        hadoopDataOutputStream.write(
            "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
        //hadoopDataOutputStream.write(message.toString().getBytes());
      } else if (hadoopFileSystem.exists(path)) {
        hadoopDataOutputStream = hadoopFileSystem.append(path);
        hadoopDataOutputStream.write(
            "Hello, I am appending to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
        //hadoopDataOutputStream.write(message.getBytes());
        return hadoopFileSystem;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
          hadoopDataOutputStream.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    return hadoopFileSystem;
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

      LOG.info("Directory String Is:%%%" + directoryString + "\t"
          + "Output File Is:%%%" + outputFile);

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
          hadoopDataOutputStream.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}
