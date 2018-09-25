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
import edu.iu.dsc.tws.common.config.TokenSub;

public class SchedulerContext extends Context {
  public static final String LAUNCHER_CLASS = "twister2.class.launcher";
  public static final String UPLOADER_CLASS = "twister2.class.uploader";
  public static final String WORKER_CLASS = "twister2.job.worker.class";
  public static final String THREADS_PER_WORKER = "twister2.exector.worker.threads";

  public static final String SYSTEM_PACKAGE_URI = "twister2.system.package.uri";

  // Internal configuration for job package url
  public static final String JOB_PACKAGE_URI = "twister2.job.package.uri";

  // Temp directory where the files are placed before packing them for upload
  public static final String JOB_TEMP_DIR = "twister2.client.job.temp.dir";

  /**
   * These are specified as system properties when deploying a job
   */
  public static final String TWISTER_2_HOME = "twister2_home";
  public static final String CONFIG_DIR = "config_dir";
  public static final String CLUSTER_TYPE = "cluster_type";
  public static final String USER_JOB_JAR_FILE = "job_file";
  public static final String JOB_DESCRIPTION_FILE_CMD_VAR = "job_desc_file";

  public static final String WORKING_DIRECTORY = "twister2.working_directory";

  public static final String CORE_PACKAGE_FILENAME_DEFAULT = "twister2-core.tar.gz";
  public static final String CORE_PACKAGE_FILENAME = "twister2.package.core";

  public static final String JOB_PACKAGE_FILENAME_DEFAULT = "twister2-job.tar.gz";
  public static final String JOB_PACKAGE_FILENAME = "twister2.package.job";

  // The path from where the workers will transfer twister2 tar.gz packages
  public static final String TWISTER2_PACKAGES_PATH = "twister2.packages.path";
  // local temporary packages path on the submitting client
  public static final String TEMPORARY_PACKAGES_PATH = "temporary.packages.path";

  public static final String NFS_SERVER_ADDRESS = "nfs.server.address";
  public static final String NFS_SERVER_PATH = "nfs.server.path";

  // persistent volume per worker in GB
  public static final double PERSISTENT_VOLUME_PER_WORKER_DEFAULT = 0.0;
  public static final String PERSISTENT_VOLUME_PER_WORKER = "persistent.volume.per.worker";
  public static final String WORKER_END_SYNC_TIME = "twister2.worker.end.sync.wait.time.ms";

  public static String uploaderClass(Config cfg) {
    return cfg.getStringValue(UPLOADER_CLASS);
  }

  public static String launcherClass(Config cfg) {
    return cfg.getStringValue(LAUNCHER_CLASS);
  }

  public static String workerClass(Config cfg) {
    return cfg.getStringValue(WORKER_CLASS);
  }

  public static String packagesPath(Config cfg) {
    return cfg.getStringValue(TWISTER2_PACKAGES_PATH);
  }

  public static String temporaryPackagesPath(Config cfg) {
    return cfg.getStringValue(TEMPORARY_PACKAGES_PATH);
  }

  public static String systemPackageUrl(Config cfg) {
    return TokenSub.substitute(cfg, cfg.getStringValue(SYSTEM_PACKAGE_URI,
        "${TWISTER2_DIST}/twister2-core.tar.gz"), Context.substitutions);
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

  public static String nfsServerAddress(Config cfg) {
    return cfg.getStringValue(NFS_SERVER_ADDRESS);
  }

  public static String nfsServerPath(Config cfg) {
    return cfg.getStringValue(NFS_SERVER_PATH);
  }

  public static double persistentVolumePerWorker(Config cfg) {
    return cfg.getDoubleValue(PERSISTENT_VOLUME_PER_WORKER, PERSISTENT_VOLUME_PER_WORKER_DEFAULT);
  }

  /**
   * if persistentVolumePerWorker is more than zero, return true, otherwise false
   */
  public static boolean persistentVolumeRequested(Config cfg) {
    return persistentVolumePerWorker(cfg) > 0;
  }

  /**
   * if workerVolatileDisk is more than zero, return true, otherwise false
   */
  public static boolean volatileDiskRequested(Config cfg) {
    return workerVolatileDisk(cfg) > 0;
  }

  public static String createJobDescriptionFileName(String jobName) {
    return jobName + ".job";
  }

  public static int workerEndSyncWaitTime(Config cfg) {
    return cfg.getIntegerValue("twister2.worker.end.sync.time.ms", 30000);
  }
}
