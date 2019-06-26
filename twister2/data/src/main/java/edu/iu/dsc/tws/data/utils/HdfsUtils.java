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
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;

/**
 * This is the util class for creating the Hadoop Distributed File System, getting the path,
 * length of the file, and locate the datanode location of the input files.
 */
public class HdfsUtils {

  private static final Logger LOG = Logger.getLogger(HdfsUtils.class.getName());

  private Config config;
  private String fileName;

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
}
