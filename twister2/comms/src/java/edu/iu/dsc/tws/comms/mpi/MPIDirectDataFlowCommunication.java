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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * A direct data flow operation sends peer to peer messages
 */
public class MPIDirectDataFlowCommunication extends MPIDataFlowOperation {
  private Set<Integer> sources;
  private int destination;
  private DirectRouter router;
  private MessageReceiver finalReceiver;

  public MPIDirectDataFlowCommunication(TWSMPIChannel channel,
                                        Set<Integer> srcs, int dest,
                                        MessageReceiver finalRcvr) {
    super(channel);

    this.sources = srcs;
    this.destination = dest;
    this.finalReceiver = finalRcvr;
  }

  @Override
  protected void setupRouting() {
    this.router = new DirectRouter(instancePlan, sources, destination);

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
      pendingReceiveDeSerializations.put(s, new ArrayBlockingQueue<MPIMessage>(
          MPIContext.sendPendingMax(config)));
    }
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return router.isLastReceiver();
  }

  @Override
  protected boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();
    // check weather this message is for a sub task
    return finalReceiver.onMessage(header.getSourceId(), 0, destination, header.getFlags(), object);
  }

  @Override
  protected boolean receiveSendInternally(int source, int t, int path, int flags, Object message) {
    // we only have one destination in this case
    if (t != destination) {
      throw new RuntimeException("We only have one destination");
    }

    // okay this must be for the
    return finalReceiver.onMessage(source, path, t, flags, message);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return this.router.receiveExpectedTaskIds();
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("This method is not used by direct communication");
  }

  @Override
  protected boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    // get the expected routes
    Map<Integer, Set<Integer>> internalRoutes = router.getInternalSendTasks(source);
    if (internalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> internalSourceRouting = internalRoutes.get(source);
    if (internalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addInternalRoutes(internalSourceRouting);
    }

    // get the expected routes
    Map<Integer, Set<Integer>> externalRouting = router.getExternalSendTasks(source);
    if (externalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> externalSourceRouting = externalRouting.get(source);
    if (externalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addExternalRoutes(externalSourceRouting);
    }
    routingParameters.setDestinationId(destination);
    return routingParameters;
  }
}
