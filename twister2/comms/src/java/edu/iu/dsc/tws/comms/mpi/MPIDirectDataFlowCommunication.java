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

import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.routing.IRouter;

/**
 * A direct data flow operation sends peer to peer messages
 */
public class MPIDirectDataFlowCommunication extends MPIDataFlowOperation {
  private Set<Integer> sources;
  private int destination;

  public MPIDirectDataFlowCommunication(TWSMPIChannel channel,
                                        Set<Integer> srcs, int dest) {
    super(channel);

    this.sources = srcs;
    this.destination = dest;
  }

  @Override
  protected IRouter setupRouting() {
    return new DirectRouter(instancePlan, sources, destination);
  }

  /**
   * We will use the destination task id as the identifier
   * @return
   */
  @Override
  protected int destinationIdentifier() {
    return destination;
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("We are not routing received messages");
  }

  @Override
  protected void routeSendMessage(int source, MPISendMessage message, List<Integer> routes) {
    Set<Integer> downstreamTasks = router.getDownstreamTasks(source);
    routes.addAll(downstreamTasks);
  }
}
