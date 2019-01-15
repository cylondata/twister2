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

import java.util.LinkedList;
import java.util.List;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.master.server.WorkerMonitor;

public class DriverMessenger implements IDriverMessenger {

  private WorkerMonitor workerMonitor;

  public DriverMessenger(WorkerMonitor workerMonitor) {
    this.workerMonitor = workerMonitor;
  }

  /**
   * send this message to all workers in the job
   * @return
   */
  @Override
  public boolean broadcastToAllWorkers(Message message) {
    return workerMonitor.broadcastMessage(message);
  }

  @Override
  public boolean sendToWorkerList(Message message, List<Integer> workerList) {

    return workerMonitor.sendMessageToWorkerList(message, workerList);
  }

  @Override
  public boolean sendToWorker(Message message, Integer workerID) {
    LinkedList<Integer> workerList = new LinkedList<>();
    workerList.add(workerID);
    return workerMonitor.sendMessageToWorkerList(message, workerList);
  }
}
