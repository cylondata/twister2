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
package edu.iu.dsc.tws.rsched.schedulers.mesos.master;

import java.io.File;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosContext;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

public class JobTerminator implements IJobTerminator {
  public static final Logger LOG = Logger.getLogger(JobTerminator.class.getName());
  private Config config;
  private String frameworkId;
  public JobTerminator(Config cfg, String frameworkId) {
    config = cfg;
    this.frameworkId = frameworkId;
  }
  @Override
  //mesos needs frameworkd Id to kill it
  public boolean terminateJob(String jobName) {

//    MesosController.schedulerDriver.killTask(Protos.TaskID.newBuilder()
//        .setValue(Integer.toString(2)).build());
//

//    String frameworkKillCommand2 = "curl -d frameworkId="
//        + frameworkId
//        + " -X POST http://"
//        + MesosContext.getMesosMasterHost(config) + ":5050/";
//    ProcessUtils.runSyncProcess(false,
//        frameworkKillCommand2.split(" "), new StringBuilder(),
//        new File("."), true);
//
//    try {
//      Thread.sleep(10000);
//    } catch (InterruptedException e) {
//
//    }

    String frameworkKillCommand = "curl -XPOST http://"
        + MesosContext.getMesosMasterHost(config) + ":5050/master/teardown -d frameworkId="
        + frameworkId;
    System.out.println("kill command:" + frameworkKillCommand);

    ProcessUtils.runSyncProcess(false,
        frameworkKillCommand.split(" "), new StringBuilder(),
        new File("."), true);

    return true;
  }

}
