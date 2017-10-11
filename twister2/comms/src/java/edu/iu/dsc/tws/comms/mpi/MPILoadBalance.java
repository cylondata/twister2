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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.LoadBalanceRouter;
import edu.iu.dsc.tws.comms.routing.Routing;

public class MPILoadBalance extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPILoadBalance.class.getName());

  private Random random;

  protected Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  public MPILoadBalance(TWSMPIChannel channel) {
    super(channel);
    random = new Random(System.nanoTime());
  }

  @Override
  public void sendPartialMessage(Message message) {
    throw new UnsupportedOperationException("partial messages not supported by load balance");
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException("partial messages not supported by load balance");
  }


  protected IRouter setupRouting() {
    // lets create the routing needed
    LoadBalanceRouter router = new LoadBalanceRouter();
    router.init(config, thisTask, instancePlan, sources, destinations, stream,
        MPIContext.distinctRoutes(config, sources.size()));
    return router;
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("Load-balance doesn't rout received messages");
  }

  @Override
  protected void routeSendMessage(MessageHeader message, List<Integer> routes) {
    Routing routing = expectedRoutes.get(thisTask);

    int next = random.nextInt(routing.getDownstreamIds().size());
    routes.add(routing.getDownstreamIds().get(next));
  }

  @Override
  protected void sendCompleteMPIMessage(MPIMessage mpiMessage) {
    MessageHeader header = mpiMessage.getHeader();

    if (header.getSourceId() != thisTask) {
      throw new RuntimeException("The source of the message should be the sender");
    }

    List<Integer> routes = new ArrayList<>();
    routeSendMessage(header, routes);
    if (routes.size() == 0) {
      throw new RuntimeException("Failed to get downstream tasks");
    }

    sendMessage(mpiMessage, routes);
  }



  @Override
  public void onReceiveComplete(int id, int messageStream, MPIBuffer buffer) {
    int originatingNode = buffer.getByteBuffer().getInt();

    if (!sources.contains(originatingNode)) {
      throw new RuntimeException("The message should always come directly from a source");
    }

    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage currentMessage = currentMessages.get(originatingNode);

    if (currentMessage == null) {
      MessageHeader header = buildHeader(buffer);
      currentMessage = new MPIMessage(thisTask, header, MPIMessageType.RECEIVE, this);
      currentMessages.put(originatingNode, currentMessage);
    } else if (!currentMessage.isComplete()) {
      currentMessage.addBuffer(buffer);
      currentMessage.build();
    }

    if (currentMessage.isComplete()) {
      // we received a message, we need to determine weather we need to forward to another node
      // and process
      if (messageDeSerializer != null) {
        Object object = messageDeSerializer.buid(currentMessage);
        receiver.onMessage(object);
      }

      currentMessages.remove(originatingNode);
    }
  }
}
