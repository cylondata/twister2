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
package edu.iu.dsc.tws.api.driver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.JobExecutionState;

public class DefaultDriver implements IDriver {
  private static final Logger LOG = Logger.getLogger(DefaultDriver.class.getName());

  private Map<Integer, JobExecutionState.WorkerJobState> workerMessages = new HashMap<>();
  private DriverJobState state = DriverJobState.RUNNING;

  @Override
  public void execute(Config config, IScaler scaler, IDriverMessenger messenger) {
    //The default driver is not involved in the execution currently so nothing to do here
  }

  @Override
  public void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    try {
      JobExecutionState.WorkerJobState message =
          anyMessage.unpack(JobExecutionState.WorkerJobState.class);
      if (state != DriverJobState.FAILED) {
        if (message.getFailure()) {
          state = DriverJobState.FAILED;
        }
      }
      workerMessages.put(senderWorkerID, message);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Unable to unpack received protocol"
          + " buffer message as broadcast", e);
    }
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {

  }

  @Override
  public DriverJobState getState() {
    return state;
  }

  @Override
  public Map<Integer, String> getMessages() {
    Map<Integer, String> messages = new HashMap<>();
    for (Integer workerId : workerMessages.keySet()) {
      messages.put(workerId, workerMessages.get(workerId).getWorkerMessage());
    }
    return messages;
  }
}
