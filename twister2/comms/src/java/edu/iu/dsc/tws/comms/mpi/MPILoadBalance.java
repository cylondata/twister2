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

  protected IRouter setupRouting() {
    // lets create the routing needed
    LoadBalanceRouter router = new LoadBalanceRouter();
    router.init(config, thisTask, instancePlan, sources, destinations, edge,
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
}
