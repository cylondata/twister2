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

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.driver.K8sScaler;
import edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter;
import edu.iu.dsc.tws.rsched.utils.FileUtils;

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

    if (args.length != 1) {
      LOG.info("usage: java JobMasterExample numberOfWorkers");
      return;
    }

    int numberOfWorkers = Integer.parseInt(args[0]);
    String host = "0.0.0.0";

    // we assume that the twister2Home is the current directory
//    String configDir = "../twister2/config/src/yaml/";
    String configDir = "";
    String twister2Home = Paths.get(configDir).toAbsolutePath().toString();
    Config config = ConfigLoader.loadConfig(twister2Home, "conf", "kubernetes");
    config = JobMasterClientExample.updateConfig(config, config, host);
    LOG.info("Loaded: " + config.size() + " configuration parameters.");

//    Twister2Job twister2Job = Twister2Job.loadTwister2Job(config, null);
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-world-job")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(.2, 128, numberOfWorkers)
        .build();
    twister2Job.setUserName(System.getProperty("user.name"));

    JobAPI.Job job = twister2Job.serialize();
    LOG.info("JobID: " + job.getJobId());

    JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;
    JobMasterStarter.job = job;

    if (ZKContext.isZooKeeperServerUsed(config)) {
      if ("start".equalsIgnoreCase(args[0])) {

        JobMasterStarter.initializeZooKeeper(config, job.getJobId(), host, initialState);

      } else if ("restart".equalsIgnoreCase(args[0])) {

        initialState = JobMasterAPI.JobMasterState.JM_RESTARTED;
        JobMasterStarter.initializeZooKeeper(config, job.getJobId(), host, initialState);
        job = JobMasterStarter.job;

      } else {
        LOG.info("usage: java JobMasterExample start/restart");
        return;
      }
    }

    // write jobID to file
    String dir = System.getProperty("user.home") + "/.twister2";
    if (!FileUtils.isDirectoryExists(dir)) {
      FileUtils.createDirectory(dir);
    }
    String filename = dir + "/last-job-id.txt";
    FileUtils.writeToFile(filename, (job.getJobId() + "").getBytes(), true);
    LOG.info("Written jobID to file: " + job.getJobId());

    String ip = null;
    try {
      ip = Inet4Address.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return;
    }
    JobMasterAPI.NodeInfo jobMasterNode = NodeInfoUtils.createNodeInfo(ip, null, null);

    KubernetesController controller = new KubernetesController();
    K8sScaler k8sScaler = new K8sScaler(config, job, controller);
    IJobTerminator jobTerminator = null;

    JobMaster jobMaster =
        new JobMaster(config, host, jobTerminator, job, jobMasterNode, k8sScaler, initialState);
    try {
//      jobMaster.startJobMasterThreaded();
      jobMaster.startJobMasterBlocking();
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, "Exception when starting Job master: ", e);
      throw new RuntimeException(e);
    }

    LOG.info("Threaded Job Master started:"
        + "\nnumberOfWorkers: " + job.getNumberOfWorkers()
        + "\njobID: " + job.getJobId()
    );

  }
}
