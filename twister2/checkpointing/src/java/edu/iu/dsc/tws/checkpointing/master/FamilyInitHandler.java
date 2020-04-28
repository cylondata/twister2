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
package edu.iu.dsc.tws.checkpointing.master;

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class FamilyInitHandler {

  private static final Logger LOG = Logger.getLogger(FamilyInitHandler.class.getName());

  private int count;
  private HashMap<Integer, RequestID> pendingResponses;
  private RRServer rrServer;
  private String family;
  private Long familyVersion;
  private boolean pause; // should be paused when cluster is unstable

  public FamilyInitHandler(RRServer rrServer,
                           String family,
                           int count, Long familyVersion) {
    this.rrServer = rrServer;
    this.family = family;
    this.familyVersion = familyVersion;
    this.pendingResponses = new HashMap<>();
    this.count = count;
  }

  public void pause() {
    this.pendingResponses.clear();
    this.pause = true;
  }

  public void resume() {
    this.pendingResponses.clear();
    this.pause = false;
  }

  public boolean scheduleResponse(int workerId, RequestID requestID) {
    if (this.pause) {
      LOG.info("Handler is in paused mode, due to cluster instability. "
          + "Ignored a request from " + workerId);
      return false;
    }
    RequestID previousRequest = this.pendingResponses.put(workerId, requestID);
    if (previousRequest != null) {
      LOG.warning("Duplicate request received for " + this.family
          + " from worker : " + workerId + ". Workers might be coming after a failure.");
    }
    if (this.pendingResponses.size() == count) {
      for (RequestID pendingRespons : this.pendingResponses.values()) {
        this.rrServer.sendResponse(pendingRespons,
            Checkpoint.FamilyInitializeResponse.newBuilder()
                .setFamily(this.family)
                .setVersion(this.familyVersion)
                .build());
      }
      this.pendingResponses.clear();
      return true;
    } else {
      return false;
    }
  }

  public long getVersion() {
    return this.familyVersion;
  }
}
