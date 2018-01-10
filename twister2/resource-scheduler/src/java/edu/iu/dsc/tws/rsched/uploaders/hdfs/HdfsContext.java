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
package edu.iu.dsc.tws.rsched.uploaders.hdfs;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

final class HdfsContext extends Context {
  public static final String HADOOP_CONFIG_DIRECTORY = "twister2.uploader.hdfs.config.directory";
  public static final String HDFS_TOPOLOGIES_DIRECTORY_URI =
      "twister2.uploader.hdfs.topologies.directory.uri";

  private HdfsContext() {
  }

  public static String hadoopConfigDirectory(Config config) {
    return config.getStringValue(HADOOP_CONFIG_DIRECTORY);
  }

  public static String hdfsTopologiesDirectoryURI(Config config) {
    return config.getStringValue(HDFS_TOPOLOGIES_DIRECTORY_URI);
  }
}
