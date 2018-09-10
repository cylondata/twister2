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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;

public final class HdfsDataContext extends Context {

  private static final String HDFS_URL = "twister2.hdfs.url";
  private static final String HDFS_URL_DEFAULT = "hdfs://hostname.iu.edu:9000//";

  public static final String HADOOP_HOME = "twister2.hadoop.home";
  public static final String HADOOP_HOME_DEFAULT = "${HADOOP_HOME}";

  private static final String HDFS_CLASS = "twister2.hdfs.class";
  private static final String HDFS_CLASS_DEFAULT = "org.apache.hadoop.hdfs.DistributedFileSystem";

  private static final String HDFS_IMPLEMENTATION_KEY = "twister2.hdfs.implementation.key";
  private static final String HDFS_IMPLEMENTATION_KEY_DEFAULT = "fs.hdfs.impl";

  private static final String HDFS_CONFIG_DIRECTORY = "twister2.hdfs.config.directory";
  private static final String HDFS_CONFIG_DIRECTORY_DEFAULT
      = "$HADOOP_HOME/etc/hadoop/core-site.xml";

  private static final String HDFS_DATA_DIRECTORY = "twister2.hdfs.data.directory";
  private static final String HDFS_DATA_DIRECTORY_DEFAULT = "/user/";

  private static final String HDFS_NAMENODE_NAME = "twister2.hdfs.namenode";
  private static final String HDFS_NAMENODE_DEFAULT = "hostname.iu.edu";

  private static final String HDFS_NAMENODE_PORT = "twister2.hdfs.namenode.port";
  private static final Integer HDFS_NAMENODE_PORT_DEFAULT = 9000;

  private HdfsDataContext() {
  }

  public static String getHdfsNamenodeDefault(Config cfg) {
    return cfg.getStringValue(HDFS_NAMENODE_NAME, HDFS_NAMENODE_DEFAULT);
  }

  public static Integer getHdfsNamenodePortDefault(Config cfg) {
    return cfg.getIntegerValue(HDFS_NAMENODE_PORT, HDFS_NAMENODE_PORT_DEFAULT);
  }

  public static String getHdfsUrlDefault(Config cfg) {
    return cfg.getStringValue(HDFS_URL, HDFS_URL_DEFAULT);
  }

  public static String getHdfsClassDefault(Config cfg) {
    return cfg.getStringValue(HDFS_CLASS, HDFS_CLASS_DEFAULT);
  }

  public static String getHdfsImplementationKey(Config cfg) {
    return cfg.getStringValue(HDFS_IMPLEMENTATION_KEY, HDFS_IMPLEMENTATION_KEY_DEFAULT);
  }

  public static String getHdfsConfigDirectory(Config config) {
    return config.getStringValue(HDFS_CONFIG_DIRECTORY, HDFS_CONFIG_DIRECTORY_DEFAULT);
  }

  public static String getHdfsDataDirectory(Config config) {
    return config.getStringValue(HDFS_DATA_DIRECTORY, HDFS_DATA_DIRECTORY_DEFAULT);
  }

  public static String getHadoopHome(Config config) {
    return TokenSub.substitute(config, config.getStringValue(HADOOP_HOME,
        HADOOP_HOME_DEFAULT), Context.substitutions);
  }
}
