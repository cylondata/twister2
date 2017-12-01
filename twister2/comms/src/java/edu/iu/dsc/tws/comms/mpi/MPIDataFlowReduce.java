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
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;

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
    // we only have one path
    this.router = new InvertedBinaryTreeRouter(config, instancePlan,
        destination, sources);
  }

  protected int destinationIdentifier(int source, int path) {
    return router.destinationIdentifier(source, path);
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return router.isLastReceiver();
  }

  /**
   * We can receive messages from internal tasks or an external task, we allways receive messages
   * to the main task of the executor and we go from there
   *
   * @param currentMessage
   * @param object
   */
  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
//    LOG.info(String.format("%d received message from %d", instancePlan.getThisExecutor(),
//        currentMessage.getHeader().getSourceId()));
    // check weather this message is for a sub task
    if (!isLast(header.getSourceId(), header.getPath(), messageDestId)
        && partialReceiver != null) {
//      LOG.info(String.format("%d calling partial receiver", instancePlan.getThisExecutor()));
      partialReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
    } else {
//      LOG.info(String.format("%d calling fina receiver", instancePlan.getThisExecutor()));
      finalReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
    }
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("We don't rout send received messages directly");
  }

  protected void externalRoutesForSend(int source, List<Integer> routes) {
    // we dont do anything
  }

  protected void externalRoutesForPartialSend(int source, List<Integer> routes) {
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> routing =
        router.getExternalSendTasksForPartial(source);
    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceRouting = routing.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routes.addAll(sourceRouting.get(0));
    }
  }

  protected void internalRouterForPartialSend(int source, List<Integer> routes) {
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> routing = router.getInternalSendTasks(source);
    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceRouting = routing.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routes.addAll(sourceRouting.get(0));
    }
  }

  @Override
  protected RoutingParameters partialSendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> internalRoutes = router.getInternalSendTasks(source);
    if (internalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceInternalRouting = internalRoutes.get(source);
    if (sourceInternalRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addInternalRoutes(sourceInternalRouting.get(0));
    }

    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> externalRoutes =
        router.getExternalSendTasksForPartial(source);
    if (externalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceRouting = externalRoutes.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addExternalRoutes(sourceRouting.get(0));
    }

    routingParameters.setDestinationId(router.destinationIdentifier(source, path));
    return routingParameters;
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();

    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> internalRouting = router.getInternalSendTasks(source);
    if (internalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    // we are going to add source if we are the main executor
    if (router.mainTaskOfExecutor(instancePlan.getThisExecutor()) == source) {
      routingParameters.addInteranlRoute(source);
    }

    // we should not have the route for main task to outside at this point
    Map<Integer, Set<Integer>> sourceInternalRouting = internalRouting.get(source);
    if (sourceInternalRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addInternalRoutes(sourceInternalRouting.get(0));
    }

    routingParameters.setDestinationId(router.destinationIdentifier(source, path));
    return routingParameters;
  }

  protected void internalRoutesForSend(int source, List<Integer> routes) {
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> routing = router.getInternalSendTasks(source);
    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    // we are going to add source if we are the main executor
    if (router.mainTaskOfExecutor(instancePlan.getThisExecutor()) == source) {
      routes.add(source);
    }

    // we should not have the route for main task to outside at this point
    Map<Integer, Set<Integer>> sourceRouting = routing.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routes.addAll(sourceRouting.get(0));
    }
  }

  @Override
  protected boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  @Override
  protected void receiveSendInternally(int source, int t, int path, Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
//      LOG.info(String.format("%d Calling directly final receiver %d",
//          instancePlan.getThisExecutor(), source));
      finalReceiver.onMessage(source, path, t, message);
    } else {
      partialReceiver.onMessage(source, path, t, message);
    }
  }

  @Override
  public boolean injectPartialResult(int source, Object message) {
    // now what we need to do
    return sendMessagePartial(source, message);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  @Override
  protected Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    Map<Integer, Map<Integer, List<Integer>>> integerMapMap = router.receiveExpectedTaskIds();
    // add the main task to receive from iteself
    int key = router.mainTaskOfExecutor(instancePlan.getThisExecutor());
    Map<Integer, List<Integer>> mainReceives = integerMapMap.get(
        key);
    List<Integer> mainReceiveList;
    if (mainReceives == null) {
      mainReceives = new HashMap<>();
      mainReceiveList = new ArrayList<>();
      mainReceives.put(key, mainReceiveList);
    } else {
      mainReceiveList = mainReceives.get(MPIContext.DEFAULT_PATH);
    }
    if (key != destination) {
      mainReceiveList.add(key);
    }

    return integerMapMap;
  }
}
