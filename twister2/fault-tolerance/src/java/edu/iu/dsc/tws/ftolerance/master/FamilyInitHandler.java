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
package edu.iu.dsc.tws.ftolerance.master;

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class FamilyInitHandler {

  private int count;
  private Set<RequestID> pendingResponses;
  private RRServer rrServer;
  private String family;
  private Long familyVersion;

  public FamilyInitHandler(RRServer rrServer,
                           String family,
                           int count, Long familyVersion) {
    this.rrServer = rrServer;
    this.family = family;
    this.familyVersion = familyVersion;
    this.pendingResponses = new HashSet<>();
    this.count = count;
  }

  public boolean scheduleResponse(RequestID requestID) {
    this.pendingResponses.add(requestID);
    if (this.pendingResponses.size() == count) {
      for (RequestID pendingRespons : this.pendingResponses) {
        this.rrServer.sendResponse(pendingRespons,
            Checkpoint.FamilyInitializeResponse.newBuilder()
                .setFamily(this.family)
                .setVersion(this.familyVersion)
                .build());
      }
      return true;
    } else {
      return false;
    }
  }

  public long getVersion() {
    return this.familyVersion;
  }
}
