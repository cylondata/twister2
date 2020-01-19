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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.TokenSub;

public final class HdfsDataContext extends Context {

  private static final String TWISTER2_DATA_HADOOP_HOME = "twister2.data.hadoop.home";
  private static final String TWISTER2_DATA_HADOOP_HOME_DEFAULT = "${HADOOP_HOME}";

  private static final String TWISTER2_DATA_HDFS_CONFIG_DIRECTORY
      = "twister2.data.hdfs.config.directory";
  private static final String TWISTER2_DATA_HDFS_CONFIG_DIRECTORY_DEFAULT
      = "$HADOOP_HOME/etc/hadoop/core-site.xml";

  private static final String TWISTER2_DATA_HDFS_DATA_DIRECTORY
      = "twister2.data.hdfs.data.directory";
  private static final String TWISTER2_DATA_HDFS_DATA_DIRECTORY_DEFAULT
       = "$HADOOP_HOME/etc/hadoop/hdfs-site.xml";

  private static final String TWISTER2_DATA_HDFS_NAMENODE = "twister2.data.hdfs.namenode";
  private static final String TWISTER2_DATA_HDFS_NAMENODE_DEFAULT
      = "hostname.domain.name";

  private static final String TWISTER2_DATA_HDFS_NAMENODE_PORT = "twister2.data.hdfs.namenode.port";
  private static final Integer TWISTER2_DATA_NAMENODE_PORT_DEFAULT = 9000;

  private HdfsDataContext() {
  }

  public static String getHdfsNamenodeDefault(Config cfg) {
    return cfg.getStringValue(TWISTER2_DATA_HDFS_NAMENODE, TWISTER2_DATA_HDFS_NAMENODE_DEFAULT);
  }

  public static Integer getHdfsNamenodePortDefault(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_DATA_HDFS_NAMENODE_PORT,
        TWISTER2_DATA_NAMENODE_PORT_DEFAULT);
  }

  public static String getHdfsConfigDirectory(Config config) {
    return config.getStringValue(TWISTER2_DATA_HDFS_CONFIG_DIRECTORY,
        TWISTER2_DATA_HDFS_CONFIG_DIRECTORY_DEFAULT);
  }

  public static String getHdfsDataDirectory(Config config) {
    return config.getStringValue(TWISTER2_DATA_HDFS_DATA_DIRECTORY,
        TWISTER2_DATA_HDFS_DATA_DIRECTORY_DEFAULT);
  }

  public static String getHadoopHome(Config config) {
    return TokenSub.substitute(config, config.getStringValue(TWISTER2_DATA_HADOOP_HOME,
        TWISTER2_DATA_HADOOP_HOME_DEFAULT), Context.substitutions);
  }
}
