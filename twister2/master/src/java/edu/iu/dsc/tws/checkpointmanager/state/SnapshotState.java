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
package edu.iu.dsc.tws.checkpointmanager.state;

import java.util.HashMap;

public class SnapshotState {

  private long jobId;
  private HashMap<Integer, Snapshot> states = new HashMap<>();

  public void update(Object state) {
    //serializing the state and add it to the snapshot  with the specific nodeID
  }

  public Snapshot getState(Integer nodeID) {
    //return the deserialized snapshot
    return null;
  }
}
