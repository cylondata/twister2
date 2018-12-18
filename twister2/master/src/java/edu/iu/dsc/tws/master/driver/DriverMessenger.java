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
package edu.iu.dsc.tws.master.driver;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.driver.IDriverMessenger;

public class DriverMessenger implements IDriverMessenger {

  private JMDriverAgent driverAgent;

  public DriverMessenger(JMDriverAgent driverAgent) {
    this.driverAgent = driverAgent;
  }

  /**
   * send this message to all workers in the job
   * @return
   */
  @Override
  public boolean broadcastToAllWorkers(Message message) {
    return driverAgent.sendBroadcastMessage(message);
  }
}
