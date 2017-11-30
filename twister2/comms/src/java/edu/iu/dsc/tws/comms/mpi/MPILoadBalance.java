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
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.LoadBalanceRouter;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MPILoadBalance extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPILoadBalance.class.getName());

  private Set<Integer> sources;
  private Set<Integer> destinations;
  protected IRouter router;

  private List<Integer> destinationList;
  private Map<Integer, Integer> destinationIndex;
  private Set<Integer> thisSources;
  private Destinations dests = new Destinations();

  /**
   * A place holder for keeping the internal and external destinations
   */
  private class Destinations {
    int total = 0;
    List<Integer> internal = new ArrayList<>();
    List<Integer> external = new ArrayList<>();
  }

  public MPILoadBalance(TWSMPIChannel channel, Set<Integer> srcs, Set<Integer> dests) {
    super(channel);
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();

    for (int s : sources) {
      destinationIndex.put(s, 0);
    }
    this.destinationList = new ArrayList<>(destinations);

    this.thisSources = TaskPlanUtils.getTasksOfThisExecutor(instancePlan, srcs);
  }

  protected void setupRouting() {
    this.router = new LoadBalanceRouter(instancePlan, sources, destinations);
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("Load-balance doesn't rout received messages");
  }

  @Override
  public boolean injectPartialResult(int source, Object message) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  protected RoutingParameters partialSendRoutingParameters(int source, int path) {
    throw new RuntimeException("Method not supported");
  }

  private int getRouting(int source) {
    return 0;
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    int destination = 0;

    routingParameters.setDestinationId(destination);
    return routingParameters;
  }

  @Override
  protected void receiveSendInternally(int source, int t, int path, Object message) {
    // okay this must be for the
    finalReceiver.onMessage(source, path, t, message);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  @Override
  protected Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return destinations.contains(taskIdentifier);
  }

  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    finalReceiver.onMessage(header.getSourceId(), header.getPath(),
        router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
  }

  @Override
  protected boolean isLastReceiver() {
    return true;
  }
}
