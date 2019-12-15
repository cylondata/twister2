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
package edu.iu.dsc.tws.api.scheduler;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.driver.DriverJobState;
import edu.iu.dsc.tws.common.util.JSONUtils;

/**
 * Acts as an reference to the job once it is submitted.
 */
public class Twister2JobState {

  /**
   * True of the job request was granted
   */
  private boolean requestGranted;

  /**
   * Specifies if the job was submitted in detached mode. In this mode the job tracking
   * information is not available trough the state object.
   */
  private boolean isDetached;


  /**
   * The state of the job, since the driver represents the current job, the driver state is
   * taken as the job state
   */
  private DriverJobState jobstate;

  /**
   * Final messages received from each worker
   */
  private Map<Integer, String> messages;


  public Twister2JobState(boolean granted) {
    this.requestGranted = granted;
    this.isDetached = true;
    this.jobstate = DriverJobState.RUNNING;
    this.messages = new HashMap<>();
  }

  public boolean isRequestGranted() {
    return requestGranted;
  }

  public void setRequestGranted(boolean requestGranted) {
    this.requestGranted = requestGranted;
  }

  public boolean isDetached() {
    return isDetached;
  }

  public void setDetached(boolean detached) {
    isDetached = detached;
  }

  public DriverJobState getJobstate() {
    return jobstate;
  }

  public void setJobstate(DriverJobState jobstate) {
    this.jobstate = jobstate;
  }

  public void setFinalMessages(Map<Integer, String> finalMessages) {
    this.messages = finalMessages;
  }

  public Map<Integer, String> getMessages() {
    return messages;
  }

  public Exception getCause() {
    if (jobstate == DriverJobState.FAILED) {
      for (Integer workerId : messages.keySet()) {
        if (!messages.get(workerId).equals("Worker Completed")) {
          Exception exception = JSONUtils.fromJSONString(messages.get(workerId), Exception.class);
          return exception;
        }
      }
    }
    return null;
  }
}
