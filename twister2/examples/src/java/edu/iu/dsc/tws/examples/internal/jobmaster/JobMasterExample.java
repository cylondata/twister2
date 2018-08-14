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

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;

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

    if (args.length != 1) {
      printUsage();
      return;
    }

    int numberOfWorkers = Integer.parseInt(args[0]);
    Config configs = buildConfig(numberOfWorkers);

    LOG.info("Config parameters: \n" + configs);

    String host = JobMasterContext.jobMasterIP(configs);
    String jobName = Context.jobName(configs);

    JobMaster jobMaster = new JobMaster(configs, host, null, jobName);
    jobMaster.startJobMasterThreaded();

    LOG.info("Threaded Job Master started.");
  }

  /**
   * construct a Config object
   * @return
   */
  public static Config buildConfig(int numberOfWorkers) {
    return Config.newBuilder()
        .put(JobMasterContext.JOB_MASTER_IP, "localhost")
        .put(Context.JOB_NAME, "basic-kube")
        .put(Context.TWISTER2_WORKER_INSTANCES, numberOfWorkers)
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS, "true")
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java JobMasterExample numberOfWorkers");
  }

}
