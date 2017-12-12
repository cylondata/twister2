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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.routing.LoadBalanceRouter;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MPILoadBalance extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPILoadBalance.class.getName());

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private LoadBalanceRouter router;

  private Map<Integer, Integer> destinationIndex;
  private Set<Integer> thisSources;
  private Destinations dests = new Destinations();
  private List<Integer> destinationsList;
  private Set<Integer> thisTasks;
  private MessageReceiver finalReceiver;

  /**
   * A place holder for keeping the internal and external destinations
   */
  @SuppressWarnings("VisibilityModifier")
  private class Destinations {
    List<Integer> internal = new ArrayList<>();
    List<Integer> external = new ArrayList<>();
  }

  public MPILoadBalance(TWSMPIChannel channel, Set<Integer> srcs,
                        Set<Integer> dests, MessageReceiver finalRcvr) {
    super(channel);
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);

    for (int s : sources) {
      destinationIndex.put(s, 0);
    }

    this.finalReceiver = finalRcvr;
  }

  protected void setupRouting() {
    this.thisSources = TaskPlanUtils.getTasksOfThisExecutor(instancePlan, sources);
    LOG.info(String.format("%d setup loadbalance routing %s",
        instancePlan.getThisExecutor(), thisSources));
    this.thisTasks = instancePlan.getTasksOfThisExecutor();
    this.router = new LoadBalanceRouter(instancePlan, sources, destinations);
    Map<Integer, Set<Integer>> internal = router.getInternalSendTasks(0);
    Map<Integer, Set<Integer>> external = router.getExternalSendTasks(0);

    LOG.info(String.format("%d adding internal/external routing", instancePlan.getThisExecutor()));
    try {
      for (int s : thisSources) {
        Set<Integer> integerSetMap = internal.get(s);
        if (integerSetMap != null) {
          this.dests.internal.addAll(integerSetMap);
        }

        Set<Integer> integerSetMap1 = external.get(s);
        if (integerSetMap1 != null) {
          this.dests.external.addAll(integerSetMap1);
        }
        LOG.info(String.format("%d adding internal/external routing %d",
            instancePlan.getThisExecutor(), s));
        break;
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
    LOG.info(String.format("%d done adding internal/external routing",
        instancePlan.getThisExecutor()));

    if (this.finalReceiver != null && isLastReceiver()) {
      this.finalReceiver.init(receiveExpectedTaskIds());
    }

    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisExecutor(instancePlan, sources);
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(config));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
    }
  }

  @Override
  public boolean sendPartial(int source, Object message) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  protected RoutingParameters partialSendRoutingParameters(int source, int path) {
    throw new RuntimeException("Method not supported");
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    int destination = 0;

    routingParameters.setDestinationId(destination);

    if (!destinationIndex.containsKey(source)) {
      throw new RuntimeException(String.format(
          "Un-expected source %d in loadbalance executor %d %s", source,
          executor, destinationIndex));
    }

    int index = destinationIndex.get(source);
    int route = destinationsList.get(index);

    if (thisTasks.contains(route)) {
      routingParameters.addInteranlRoute(route);
    } else {
      routingParameters.addExternalRoute(route);
    }
    routingParameters.setDestinationId(route);
//    LOG.info(String.format("%d Eending to %d: %d %s",
//        instancePlan.getThisExecutor(), index, route, destinationIndex));

    index = (index + 1) % destinations.size();
    destinationIndex.put(source, index);
//    LOG.info(String.format("%d EEending to %d: %d %s",
//        instancePlan.getThisExecutor(), index, route, destinationIndex));
    return routingParameters;
  }

  @Override
  protected boolean receiveSendInternally(int source, int t, int path, Object message) {
    // okay this must be for the
    return finalReceiver.onMessage(source, path, t, message);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return destinations.contains(taskIdentifier);
  }

  @Override
  protected boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    return finalReceiver.onMessage(header.getSourceId(), header.getPath(),
        router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
            header.getPath()), object);
  }

  @Override
  protected boolean isLastReceiver() {
    return true;
  }
}
