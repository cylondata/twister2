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

import edu.iu.dsc.tws.common.worker.IVolatileVolume;

public class MesosVolatileVolume implements IVolatileVolume {

  public static final String WORKER_DIR_NAME_PREFIX = "worker-";
  public static final String LOG_FILE_NAME_PREFIX = "worker-";
  public static final String LOG_DIR_NAME = "/logs";

  private String volatileJobDirPath;
  private String workerDirPath;

  private String logFileName;
  private String logDirPath;

  public MesosVolatileVolume(String volatileJobDirPath, int workerID) {
    this.volatileJobDirPath = volatileJobDirPath;
    workerDirPath = volatileJobDirPath + "/" + WORKER_DIR_NAME_PREFIX + workerID;
    logDirPath = volatileJobDirPath + LOG_DIR_NAME;
    logFileName = logDirPath + "/" + LOG_FILE_NAME_PREFIX + workerID + ".log";
    createLogDir();
  }

  private void createLogDir() {
    File logDir = new File(logDirPath);
    if (!logDir.exists()) {
      logDir.mkdirs();
    }
  }

  @Override
  public String getWorkerDirPath() {
    return workerDirPath;
  }
  public boolean jobDirExists() {
    return new File(volatileJobDirPath).exists();
  }

  @Override
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
}
