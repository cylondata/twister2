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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.LoadBalanceRouter;
import edu.iu.dsc.tws.comms.routing.Routing;

public class MPILoadBalance extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPILoadBalance.class.getName());

  private Random random;
  private Set<Integer> sources;
  private Set<Integer> destinations;

  protected Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  public MPILoadBalance(TWSMPIChannel channel, Set<Integer> srcs, Set<Integer> dests) {
    super(channel);
    random = new Random(System.nanoTime());
    this.sources = srcs;
    this.destinations = dests;
  }

  protected IRouter setupRouting() {
    // lets create the routing needed
    return new LoadBalanceRouter(config, instancePlan, sources, destinations, edge,
        MPIContext.distinctRoutes(config, sources.size()));
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("Load-balance doesn't rout received messages");
  }

  @Override
  public void injectPartialResult(int source, Object message) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  protected void routeSendMessage(int source, MPISendMessage message, List<Integer> routes) {
    Routing routing = expectedRoutes.get(source);

    int next = random.nextInt(routing.getDownstreamIds().size());
    routes.add(routing.getDownstreamIds().get(next));
  }
}
