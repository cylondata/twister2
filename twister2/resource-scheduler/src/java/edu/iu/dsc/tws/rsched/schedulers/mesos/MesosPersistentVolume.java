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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.io.File;

import edu.iu.dsc.tws.common.worker.IPersistentVolume;

public class MesosPersistentVolume implements IPersistentVolume {

  public static final String WORKER_DIR_NAME_PREFIX = "worker-";
  public static final String LOG_FILE_NAME_PREFIX = "worker-";
  public static final String LOG_DIR_NAME = "/logs";

  private String jobDirPath;
  private String workerDirPath;
  private String logFileName;
  private String logDirPath;


  public MesosPersistentVolume(String jobDirPath, int workerID) {
    this.jobDirPath = jobDirPath;
    workerDirPath = jobDirPath + "/" + WORKER_DIR_NAME_PREFIX + workerID;
    logDirPath = jobDirPath + LOG_DIR_NAME;
    logFileName = logDirPath + "/" + LOG_FILE_NAME_PREFIX + workerID + ".log";
    createLogDir();
  }

  private void createLogDir() {
    File logDir = new File(logDirPath);
    if (!logDir.exists()) {
      logDir.mkdirs();
    }
  }

  public String getJobDirPath() {
    return jobDirPath;
  }

  public String getWorkerDirPath() {
    return workerDirPath;
  }

  public boolean jobDirExists() {
    return new File(jobDirPath).exists();
  }

  public boolean workerDirExists() {
    return new File(workerDirPath).exists();
  }

  public File getJobDir() {
    File dir = new File(jobDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    return dir;
  }

  public File getWorkerDir() {
    if (!jobDirExists()) {
      return null;
    }

    File workerDir = new File(workerDirPath);
    if (workerDir.exists()) {
      return workerDir;
    } else {
      workerDir.mkdir();
      return workerDir;
    }
  }

  public String getLogFileName() {
    return logFileName;
  }


}
