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
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPIDataFlowBroadcast extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  private int source;

  private Set<Integer> destinations;

  protected IRouter router;

  public MPIDataFlowBroadcast(TWSMPIChannel channel, int src, Set<Integer> dests) {
    super(channel);
    this.source = src;
    this.destinations = dests;
  }

  @Override
  public void close() {
  }

  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
//    LOG.info(String.format("%d received message from %d", instancePlan.getThisExecutor(),
//        currentMessage.getHeader().getSourceId()));
    // check weather this message is for a sub task

//      LOG.info(String.format("%d calling fina receiver", instancePlan.getThisExecutor()));
    finalReceiver.onMessage(header.getSourceId(), header.getPath(),
        router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
  }

  @Override
  public boolean sendPartial(int src, Object message) {
    throw new RuntimeException("Not supported method");
  }

  protected void passMessageDownstream(Object object, MPIMessage currentMessage) {
    int src = router.mainTaskOfExecutor(instancePlan.getThisExecutor());
    RoutingParameters routingParameters = sendRoutingParameters(src, MPIContext.DEFAULT_PATH);

//    internalRoutesForSend(src, internalRoutes);

//    LOG.info(String.format("%d down internal routes for send %d: %s",
//        instancePlan.getThisExecutor(), src, internalRoutes));
//
//    // now lets get the external routes to send
//    externalRoutesForSend(src, externalRoutes);
//    LOG.info(String.format("%d down External routes for send %d: %s",
//        instancePlan.getThisExecutor(), src, externalRoutes));
    // we need to serialize for sending over the wire
    // LOG.log(Level.INFO, "Sending message of type: " + type);
    // this is a originating message. we are going to put ref count to 0 for now and
    // increment it later
    MPIMessage mpiMessage = new MPIMessage(src, type, MPIMessageDirection.OUT, this);

    // create a send message to keep track of the serialization
    // at the intial stage the sub-edge is 0
    int di = -1;
    if (routingParameters.getExternalRoutes().size() > 0) {
      di = routingParameters.getDestinationId();
    }
    MPISendMessage sendMessage = new MPISendMessage(src, mpiMessage, edge,
        di, MPIContext.DEFAULT_PATH, routingParameters.getInternalRoutes(),
        routingParameters.getExternalRoutes());

    // now try to put this into pending
    pendingSendMessages.offer(
        new ImmutablePair<Object, MPISendMessage>(object, sendMessage));
  }

  protected void setupRouting() {
    // we will only have one distinct route
    router = new BinaryTreeRouter(config, instancePlan, source, destinations);
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int s, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> internalRouting = router.getInternalSendTasks(source);
    if (internalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + s);
    }

    Map<Integer, Set<Integer>> internalSourceRouting = internalRouting.get(s);
    if (internalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addInternalRoutes(internalSourceRouting.get(0));
    }

    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> externalRouting = router.getExternalSendTasks(s);
    if (externalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + s);
    }
    Map<Integer, Set<Integer>> externalSourceRouting = externalRouting.get(s);
    if (externalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addExternalRoutes(externalSourceRouting.get(0));
    }
    return routingParameters;
  }

  @Override
  protected void receiveSendInternally(int src, int t, int path, Object message) {
    finalReceiver.onMessage(src, path, t, message);
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
  protected boolean isLast(int src, int path, int taskIdentifier) {
    return false;
  }

  @Override
  protected boolean isLastReceiver() {
    return true;
  }
}

