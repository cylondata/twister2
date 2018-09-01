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

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;

public final class JobMasterStarter {
  private static final Logger LOG = Logger.getLogger(JobMasterStarter.class.getName());

  private JobMasterStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    Config envConfigs = buildConfigFromEnvVariables();
    String jobName = Context.jobName(envConfigs);

    K8sWorkerUtils.initLogger(envConfigs, "jobMaster");

    LOG.info("JobMaster is starting. Current time: " + System.currentTimeMillis());
    LOG.info("Received parameters as environment variables: \n" + envConfigs);

    String host = JobMasterContext.jobMasterIP(envConfigs);
    String namespace = KubernetesContext.namespace(envConfigs);
    JobTerminator jobTerminator = new JobTerminator(namespace);

    JobMaster jobMaster = new JobMaster(envConfigs, host, jobTerminator, jobName);
    jobMaster.startJobMasterBlocking();
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
        .put(JobMasterContext.PERSISTENT_VOLUME,
            System.getenv(JobMasterContext.PERSISTENT_VOLUME))
        .put(Context.TWISTER2_WORKER_INSTANCES, System.getenv(Context.TWISTER2_WORKER_INSTANCES))
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS,
            System.getenv(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS))
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

}
