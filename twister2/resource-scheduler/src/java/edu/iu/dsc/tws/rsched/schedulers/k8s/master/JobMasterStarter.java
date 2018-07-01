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
package edu.iu.dsc.tws.rsched.schedulers.k8s.master;

import java.io.File;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.KubernetesWorkerStarter;

public final class JobMasterStarter {
  private static final Logger LOG = Logger.getLogger(JobMasterStarter.class.getName());

  private JobMasterStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    Config envConfigs = buildConfigFromEnvVariables();
    String jobName = Context.jobName(envConfigs);
    String podName = KubernetesUtils.createJobMasterPodName(jobName);

    String persistentJobDir = KubernetesContext.persistentJobDirectory(envConfigs);
    // create persistent job dir if there is a persistent volume request
    if (persistentJobDir == null || persistentJobDir.isEmpty()) {
      // no persistent volume is requested, nothing to be done
    } else {
      KubernetesWorkerStarter.createPersistentJobDir(podName, persistentJobDir, 0);
    }

    initLogger(envConfigs);

    LOG.info("JobMaster is starting. Current time: " + System.currentTimeMillis());
    LOG.info("Received parameters as environment variables: \n" + envConfigs);

    String host = JobMasterContext.jobMasterIP(envConfigs);
    String namespace = KubernetesContext.namespace(envConfigs);
    JobTerminator jobTerminator = new JobTerminator(namespace);

    JobMaster jobMaster = new JobMaster(envConfigs, host, jobTerminator, jobName);
    jobMaster.init();

    try {
      Thread.sleep(50000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    waitIndefinitely();
  }


  /**
   * construct a Config object from environment variables
   * @return
   */
  public static Config buildConfigFromEnvVariables() {
    return Config.newBuilder()
        .put(JobMasterContext.JOB_MASTER_IP, System.getenv(JobMasterContext.JOB_MASTER_IP))
        .put(JobMasterContext.JOB_MASTER_PORT, System.getenv(JobMasterContext.JOB_MASTER_PORT))
        .put(Context.JOB_NAME, System.getenv(Context.JOB_NAME))
        .put(KubernetesContext.KUBERNETES_NAMESPACE,
            System.getenv(KubernetesContext.KUBERNETES_NAMESPACE))
        .put(Context.TWISTER2_WORKER_INSTANCES, System.getenv(Context.TWISTER2_WORKER_INSTANCES))
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS,
            System.getenv(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS))
        .put(KubernetesContext.PERSISTENT_JOB_DIRECTORY,
            System.getenv(KubernetesContext.PERSISTENT_JOB_DIRECTORY))
        .put(JobMasterContext.PING_INTERVAL, System.getenv(JobMasterContext.PING_INTERVAL))
        .put(LoggingContext.PERSISTENT_LOGGING_REQUESTED,
            System.getenv(LoggingContext.PERSISTENT_LOGGING_REQUESTED))
        .put(LoggingContext.LOGGING_LEVEL, System.getenv(LoggingContext.LOGGING_LEVEL))
        .put(LoggingContext.REDIRECT_SYS_OUT_ERR,
            System.getenv(LoggingContext.REDIRECT_SYS_OUT_ERR))
        .put(LoggingContext.MAX_LOG_FILE_SIZE, System.getenv(LoggingContext.MAX_LOG_FILE_SIZE))
        .put(LoggingContext.MAX_LOG_FILES, System.getenv(LoggingContext.MAX_LOG_FILES))
        .build();
  }

  /**
   * itinialize the logger
   * @param cnfg
   */
  public static void initLogger(Config cnfg) {
    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    String persistentJobDir = KubernetesContext.persistentJobDirectory(cnfg);
    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }

    // if persistent logging is requested, initialize it
    if (LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      String persistentLogDir = persistentJobDir + "/logs";
      File logDir = new File(persistentLogDir);
      if (!logDir.exists()) {
        logDir.mkdirs();
      }

      String logFileName = "jobMaster";

      LoggingHelper.setupLogging(cnfg, persistentLogDir, logFileName);

      LOG.info("Persistent logging to file initialized.");
    }
  }

  /**
   * a method to make the job master wait indefinitely
   */
  public static void waitIndefinitely() {

    while (true) {
      try {
        LOG.info("JobMasterStarter thread waiting indefinitely. Sleeping 100sec. "
            + "Time: " + new java.util.Date());
        Thread.sleep(100000);
      } catch (InterruptedException e) {
        LOG.warning("Thread sleep interrupted.");
      }
    }
  }

}
