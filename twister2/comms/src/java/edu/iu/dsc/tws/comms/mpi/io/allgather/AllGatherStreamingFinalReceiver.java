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
package edu.iu.dsc.tws.comms.mpi.io.allgather;

import edu.iu.dsc.tws.comms.mpi.MPIDataFlowBroadcast;
import edu.iu.dsc.tws.comms.mpi.io.gather.StreamingFinalGatherReceiver;

public class AllGatherStreamingFinalReceiver extends StreamingFinalGatherReceiver {
  private MPIDataFlowBroadcast broadcast;

  public AllGatherStreamingFinalReceiver(MPIDataFlowBroadcast broadcast) {
    this.broadcast = broadcast;
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    return broadcast.send(target, object, flags);
  }

  @Override
  public void progress() {

  }
}
