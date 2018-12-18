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
package edu.iu.dsc.tws.api.htgjob;

import java.util.logging.Logger;

public class Twister2HTGDriver {

  private static final Logger LOG = Logger.getLogger(Twister2HTGDriver.class.getName());

  public void execute() {
    LOG.info("I am in execute method");
  }
}

/*import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.IDriverController;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public class Twister2HTGDriver implements IDriver {

  private static final Logger LOG = Logger.getLogger(Twister2HTGSubmitter.class.getName());

  private List<HTGJobAPI.ExecuteMessage> executeMessageList;

  public Twister2HTGDriver(List<HTGJobAPI.ExecuteMessage> executeMsgList) {
    LOG.info("I am inside htg driver");
    this.executeMessageList = executeMsgList;
  }

  @Override
  public void execute(IDriverController driverController) {

    *//*for (HTGJobAPI.ExecuteMessage anExecuteMessage : executeMessageList) {

      driverController.broadcastToAllWorkers(
          Twister2HTGSubmitter.class.getName(), anExecuteMessage);

      sleep(2000);
    }*//*

    JobMasterAPI.NodeInfo nodeInfo =
        NodeInfoUtils.createNodeInfo("example.nodeIP", "rack-01", "dc-01");

    LOG.info("Broadcasting an example protocol buffer message: " + nodeInfo);
    driverController.broadcastToAllWorkers(Twister2HTGDriver.class.getName(), nodeInfo);
  }

  private static void sleep(long duration) {
    LOG.info("Sleeping " + duration + "ms............");
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}*/
