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


public class SnapshotState {

  private long jobId;
  private int nodeId;
  private Snapshot snapshot;

  public SnapshotState(long jobId, int nodeId) {
    this.jobId = jobId;
    this.nodeId = nodeId;
    this.snapshot = new Snapshot();
  }

  public void update(Object state) {
    StorableState storableState = new StorableState(state);
    this.snapshot.addState(storableState);
    //serializing the state and add it to the snapshot  with the specific nodeID
  }

  public Snapshot getState() {
    return snapshot;
  }
}
