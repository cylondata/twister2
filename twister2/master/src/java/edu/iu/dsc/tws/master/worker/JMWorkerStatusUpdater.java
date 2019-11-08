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
package edu.iu.dsc.tws.master.worker;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JMWorkerStatusUpdater implements IWorkerStatusUpdater {
  private static final Logger LOG = Logger.getLogger(JMWorkerController.class.getName());

  private JMWorkerAgent workerAgent;

  public JMWorkerStatusUpdater(JMWorkerAgent jmWorkerAgent) {
    this.workerAgent = jmWorkerAgent;
  }

  @Override
  public boolean updateWorkerStatus(JobMasterAPI.WorkerState newState) {

    if (newState == JobMasterAPI.WorkerState.COMPLETED) {
      return workerAgent.sendWorkerCompletedMessage();
    }

    LOG.severe("Unsupported state: " + newState);
    return false;
  }

  @Override
  public JobMasterAPI.WorkerState getWorkerStatusForID(int id) {
    return null;
  }
}
