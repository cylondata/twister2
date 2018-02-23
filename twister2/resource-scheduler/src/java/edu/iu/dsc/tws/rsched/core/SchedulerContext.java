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
package edu.iu.dsc.tws.rsched.core;

import java.net.URI;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class SchedulerContext extends Context {
  public static final String STATE_MANAGER_CLASS = "twister2.class.state.manager";
  public static final String SCHEDULER_CLASS = "twister2.class.scheduler";
  public static final String LAUNCHER_CLASS = "twister2.class.launcher";
  public static final String UPLOADER_CLASS = "twister2.class.uploader";
  public static final String CONTAINER_CLASS = "twister2.job.basic.container.class";

  public static final String JOB_NAME = "twister2.job.name";
  public static final String STATE_MANAGER_ROOT_PATH = "twister2.state.manager.root.path";
  public static final String SYSTEM_PACKAGE_URI = "twister2.system.package.uri";
  /**
   * Internal configuration for job package url
   */
  public static final String JOB_PACKAGE_URI = "twister2.job.package.uri";

  /**
   * Temp directory where the files are placed before packing them for upload
   */
  public static final String JOB_TEMP_DIR = "twister2.client.job.temp.dir";

  /**
   * These are specified as system properties when deploying a topology
   */
  public static final String TWISTER_2_HOME = "twister2_home";
  public static final String CONFIG_DIR = "config_dir";
  public static final String CLUSTER_TYPE = "cluster_type";
  public static final String USER_JOB_JAR_FILE = "job_file";
  public static final String JOB_DESCRIPTION_FILE_CMD_VAR = "job_desc_file";
  public static final String JOB_DESCRIPTION_FILE = "twister2.job.description.file";

  public static final String WORKING_DIRECTORY = "twister2.working_directory";

  public static final String CORE_PACKAGE_FILENAME_DEFAULT = "twister2-core.tar.gz";
  public static final String CORE_PACKAGE_FILENAME = "twister2.package.core";

  public static final String JOB_PACKAGE_FILENAME_DEFAULT = "twister2-job.tar.gz";
  public static final String JOB_PACKAGE_FILENAME = "twister2.package.job";

  // The path from where the workers will transfer twister2 tar.gz packages
  public static final String TWISTER2_PACKAGES_PATH = "twister2.packages.path";
  public static final String WORKER_NAME = "twister2.worker.name";

  public static String stateManagerClass(Config cfg) {
    return cfg.getStringValue(STATE_MANAGER_CLASS);
  }

  public static String schedulerClass(Config cfg) {
    return cfg.getStringValue(SCHEDULER_CLASS);
  }

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(UPLOADER_CLASS);
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(LAUNCHER_CLASS);
  }

  public static String containerClass(Config cfg) {
    return cfg.getStringValue(CONTAINER_CLASS);
  }

  public static String jobName(Config cfg) {
    return cfg.getStringValue(JOB_NAME);
  }

  public static String jobDescriptionFile(Config cfg) {
    return cfg.getStringValue(JOB_DESCRIPTION_FILE);
  }

  public static String packagesPath(Config cfg) {
    return cfg.getStringValue(TWISTER2_PACKAGES_PATH);
  }

  public static String stateManegerRootPath(Config cfg) {
    return cfg.getStringValue(STATE_MANAGER_ROOT_PATH);
  }

  public static String systemPackageUrl(Config cfg) {
    return cfg.getStringValue(SYSTEM_PACKAGE_URI);
  }

  public static URI jobPackageUri(Config cfg) {
    return (URI) cfg.get(JOB_PACKAGE_URI);
  }

  public static String corePackageFileName(Config cfg) {
    return cfg.getStringValue(CORE_PACKAGE_FILENAME, CORE_PACKAGE_FILENAME_DEFAULT);
  }

  public static String jobPackageFileName(Config cfg) {
    return cfg.getStringValue(JOB_PACKAGE_FILENAME, JOB_PACKAGE_FILENAME_DEFAULT);
  }

  public static String jobClientTempDirectory(Config cfg) {
    return cfg.getStringValue(JOB_TEMP_DIR, "/tmp");
  }

  public static String userJobJarFile(Config cfg) {
    return cfg.getStringValue(USER_JOB_JAR_FILE);
  }

  public static String clusterType(Config cfg) {
    return cfg.getStringValue(TWISTER2_CLUSTER_TYPE);
  }


}
