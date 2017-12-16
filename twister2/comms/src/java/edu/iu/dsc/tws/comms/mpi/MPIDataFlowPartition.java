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
package edu.iu.dsc.tws.comms.mpi;

import java.util.Set;

public class MPIDataFlowPartition extends MPIDataFlowOperation {

  public MPIDataFlowPartition(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  protected void setupRouting() {

  }

  @Override
  protected boolean isLastReceiver() {
    return false;
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    return null;
  }

  @Override
  protected boolean receiveSendInternally(int source, int t, int path, Object message) {
    return false;
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return null;
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return false;
  }

  @Override
  protected boolean receiveMessage(MPIMessage currentMessage, Object object) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message) {
    return false;
  }
}
