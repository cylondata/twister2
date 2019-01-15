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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.driver.K8sScaler;

public final class JobMasterExample {
  private static final Logger LOG = Logger.getLogger(JobMasterExample.class.getName());

  private JobMasterExample() { }

  /**
   * this main method is for locally testing only
   * A JobMaster instance is started locally on the default port:
   *   edu.iu.dsc.tws.master.JobMasterContext.JOB_MASTER_PORT_DEFAULT = 11011
   *
   * numberOfWorkers to join is expected as a parameter
   *
   * When all workers joined and all have sent completed messages,
   * this server also completes and exits
   *
   * En example usage of JobMaster can be seen in:
   *    edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter
   */
  public static void main(String[] args) {

    // we assume that the twister2Home is the current directory
    String configDir = "conf/kubernetes/";
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, configDir);
    config = updateConfig(config);
    LOG.info("Loaded: " + config.size() + " parameters from configuration directory: " + configDir);

    Twister2Job twister2Job = Twister2Job.loadTwister2Job(config, null);
    JobAPI.Job job = twister2Job.serialize();

    String ip = null;
    try {
      ip = Inet4Address.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return;
    }
    JobMasterAPI.NodeInfo jobMasterNode = NodeInfoUtils.createNodeInfo(ip, null, null);

    String host = "localhost";
    KubernetesController controller = new KubernetesController();
    controller.init(KubernetesContext.namespace(config));
    K8sScaler k8sScaler = new K8sScaler(config, job, controller);

    JobMaster jobMaster = new JobMaster(config, host, null, job, jobMasterNode, k8sScaler);
    jobMaster.startJobMasterThreaded();

    LOG.info("Threaded Job Master started:"
        + "\nnumberOfWorkers: " + job.getNumberOfWorkers()
        + "\njobName: " + job.getJobName()
    );

  }

  /**
   * construct a Config object
   * @return
   */
  public static Config updateConfig(Config config) {
    return Config.newBuilder()
        .putAll(config)
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS, true)
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java JobMasterExample");
  }

}
