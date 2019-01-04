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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class JobMasterStarter {
  private static final Logger LOG = Logger.getLogger(JobMasterStarter.class.getName());

  private JobMasterStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // get environment variables
    String jobName = System.getenv(K8sEnvVariables.JOB_NAME + "");
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "");
    String hostIP = System.getenv(K8sEnvVariables.HOST_IP + "");

    String configDir = KubernetesConstants.CONFIG_MAP_VOLUME_MOUNT;
    Config config = K8sWorkerUtils.loadConfig(configDir);
    config = K8sWorkerUtils.unsetWorkerIDAssigment(config);

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobName);
    jobDescFileName = configDir + "/" + jobDescFileName;
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // init logger
    K8sWorkerUtils.initLogger(config, "jobMaster");

    LOG.info("JobMaster is starting. Current time: " + System.currentTimeMillis());
    LOG.info("Number of configuration parameters: " + config.size());

    // get podIP from localhost
    InetAddress localHost = null;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get localHost.", e);
    }
    String podIP = localHost.getHostAddress();

    // construct nodeInfo for Job Master
    JobMasterAPI.NodeInfo nodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
        ? KubernetesContext.getNodeInfo(config, hostIP)
        : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, hostIP);

    LOG.info("NodeInfo for JobMaster: " + nodeInfo);

    String namespace = KubernetesContext.namespace(config);
    JobTerminator jobTerminator = new JobTerminator(namespace);

    // start JobMaster
    JobMaster jobMaster = new JobMaster(config, podIP, jobTerminator, job, nodeInfo);
    jobMaster.addShutdownHook(false);
    jobMaster.startJobMasterBlocking();
  }

}
