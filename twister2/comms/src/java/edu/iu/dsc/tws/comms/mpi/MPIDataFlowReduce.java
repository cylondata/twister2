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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPIDataFlowReduce extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  protected int destination;

  protected IRouter router;

  public MPIDataFlowReduce(TWSMPIChannel channel, Set<Integer> sources, int destination) {
    super(channel);

    this.sources = sources;
    this.destination = destination;
  }

  public void setupRouting() {
    // we only have one destination and sources becomes destinations for creating tree
    // because this is an inverted tree from sources to destination
    Set<Integer> destinations = new HashSet<>();
    destinations.add(destination);

    // we only have one path
    this.router = new BinaryTreeRouter(config, instancePlan,
        destinations, sources, edge, 1);
  }

  @Override
  protected int destinationIdentifier() {
    return super.destinationIdentifier();
  }

  @Override
  protected boolean isLast(int taskIdentifier) {
    return false;
  }

  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
    // check weather this message is for a sub task
    if (!isLast(messageDestId) && partialReceiver != null) {
      partialReceiver.onMessage(header, object);
    } else {
      finalReceiver.onMessage(header, object);
    }
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("We don't rout send received messages directly");
  }

  @Override
  protected void routeSendMessage(int source, MPISendMessage message, List<Integer> routes) {
    // get the expected routes
    Set<Integer> routing = router.getDownstreamTasks(source);

    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }
    routes.addAll(routing);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  @Override
  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }
}
