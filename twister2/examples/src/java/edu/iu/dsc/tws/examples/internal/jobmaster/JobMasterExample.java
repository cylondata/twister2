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

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKInitialStateManager;
import edu.iu.dsc.tws.common.zk.ZKJobZnodeUtil;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.driver.K8sScaler;
import edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobTerminator;

public final class JobMasterExample {
  private static final Logger LOG = Logger.getLogger(JobMasterExample.class.getName());

  private JobMasterExample() {
  }

  /**
   * this main method is for locally testing only
   * A JobMaster instance is started locally on the default port:
   * edu.iu.dsc.tws.master.JobMasterContext.JOB_MASTER_PORT_DEFAULT = 11011
   * <p>
   * numberOfWorkers to join is expected as a parameter
   * <p>
   * When all workers joined and all have sent completed messages,
   * this server also completes and exits
   * <p>
   * En example usage of JobMaster can be seen in:
   * edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter
   */
  public static void main(String[] args) {

    // we assume that the twister2Home is the current directory

//    String configDir = "../twister2/config/src/yaml/";
    String configDir = "";
    String twister2Home = Paths.get(configDir).toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, "conf", "kubernetes");
    LOG.info("Loaded: " + config.size() + " configuration parameters.");

    Twister2Job twister2Job = Twister2Job.loadTwister2Job(config, null);
    twister2Job.setJobID(config.getStringValue(Context.JOB_ID));
    JobAPI.Job job = twister2Job.serialize();

    createJobZnode(config, job);

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
    IJobTerminator jobTerminator = new JobTerminator(config);
    JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;

    JobMaster jobMaster =
        new JobMaster(config, host, jobTerminator, job, jobMasterNode, k8sScaler, initialState);
    jobMaster.startJobMasterThreaded();

    LOG.info("Threaded Job Master started:"
        + "\nnumberOfWorkers: " + job.getNumberOfWorkers()
        + "\njobName: " + job.getJobName()
    );

  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java JobMasterExample");
  }

  public static void createJobZnode(Config conf, JobAPI.Job job) {

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(conf));
    String rootPath = ZKContext.rootNode(conf);

    if (ZKJobZnodeUtil.isThereJobZNodes(client, rootPath, job.getJobName())) {
      ZKJobZnodeUtil.deleteJobZNodes(client, rootPath, job.getJobName());
    }

    try {
      ZKJobZnodeUtil.createJobZNode(client, rootPath, job);
      ZKInitialStateManager.createJobZNode(client, rootPath, job.getJobName());

      // test job znode content reading
      JobAPI.Job readJob = ZKJobZnodeUtil.readJobZNodeBody(client, job.getJobName(), conf);
      LOG.info("JobZNode content: " + readJob);

    } catch (Exception e) {
      e.printStackTrace();
    }

//    ZKUtils.closeClient();
  }


}
