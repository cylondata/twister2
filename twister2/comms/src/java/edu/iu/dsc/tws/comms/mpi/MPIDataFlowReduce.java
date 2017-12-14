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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;

public class MPIDataFlowReduce extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  protected int destination;

  private InvertedBinaryTreeRouter router;

  private MessageReceiver finalReceiver;

  private MessageReceiver partialReceiver;

  private int index = 0;

  private int pathToUse = MPIContext.DEFAULT_PATH;

  public MPIDataFlowReduce(TWSMPIChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr, int indx, int p) {
    super(channel);
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
  }

  public MPIDataFlowReduce(TWSMPIChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr, MessageReceiver partialRcvr) {
    this(channel, sources, destination, finalRcvr, partialRcvr, 0, 0);
  }

  public void setupRouting() {
    // we only have one path
    this.router = new InvertedBinaryTreeRouter(config, instancePlan,
        destination, sources, index);

    // initialize the receive
    if (this.partialReceiver != null && !isLastReceiver()) {
      partialReceiver.init(receiveExpectedTaskIds());
    }

    if (this.finalReceiver != null && isLastReceiver()) {
      this.finalReceiver.init(receiveExpectedTaskIds());
    }

    LOG.info(String.format("%d all send tasks: %s", executor, router.sendQueueIds()));

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(config));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
    }

    int maxReceiveBuffers = MPIContext.receiveBufferCount(config);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<Pair<Object, MPIMessage>>(
              maxReceiveBuffers * 2 * receiveExecutorsSize);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
    }
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
  protected boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
//    LOG.info(String.format("%d received message from %d", instancePlan.getThisExecutor(),
//        currentMessage.getHeader().getSourceId()));
    // check weather this message is for a sub task
    if (!isLast(header.getSourceId(), header.getPath(), messageDestId)
        && partialReceiver != null) {
//      LOG.info(String.format("%d calling partial receiver", instancePlan.getThisExecutor()));
      return partialReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              MPIContext.DEFAULT_PATH), object);
    } else {
//      LOG.info(String.format("%d calling fina receiver", instancePlan.getThisExecutor()));
      return finalReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              MPIContext.DEFAULT_PATH), object);
    }
  }

  @Override
  protected RoutingParameters partialSendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    // get the expected routes
    Map<Integer, Set<Integer>> internalRoutes = router.getInternalSendTasks(source);
    if (internalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> sourceInternalRouting = internalRoutes.get(source);
    if (sourceInternalRouting != null) {
      routingParameters.addInternalRoutes(sourceInternalRouting);
    }

    // get the expected routes
    Map<Integer, Set<Integer>> externalRoutes =
        router.getExternalSendTasksForPartial(source);
    if (externalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> sourceRouting = externalRoutes.get(source);
    if (sourceRouting != null) {
      routingParameters.addExternalRoutes(sourceRouting);
    }

    routingParameters.setDestinationId(router.destinationIdentifier(source, path));
    return routingParameters;
  }

  @Override
  protected RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();

    // get the expected routes
    Map<Integer, Set<Integer>> internalRouting = router.getInternalSendTasks(source);
    if (internalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    // we are going to add source if we are the main executor
    if (router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
        MPIContext.DEFAULT_PATH) == source) {
      routingParameters.addInteranlRoute(source);
    }

    // we should not have the route for main task to outside at this point
    Set<Integer> sourceInternalRouting = internalRouting.get(source);
    if (sourceInternalRouting != null) {
      routingParameters.addInternalRoutes(sourceInternalRouting);
    }

    routingParameters.setDestinationId(router.destinationIdentifier(source, path));
    return routingParameters;
  }

  @Override
  protected boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  @Override
  protected boolean receiveSendInternally(int source, int t, int path, Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
//      LOG.info(String.format("%d Calling directly final receiver %d",
//          instancePlan.getThisExecutor(), source));
      return finalReceiver.onMessage(source, path, t, message);
    } else {
      return partialReceiver.onMessage(source, path, t, message);
    }
  }

  @Override
  public boolean send(int source, Object message) {
    return sendMessage(source, message, pathToUse);
  }

  @Override
  public boolean sendPartial(int source, Object message) {
    // now what we need to do
    return sendMessagePartial(source, message, pathToUse);
  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    Map<Integer, List<Integer>> integerMapMap = router.receiveExpectedTaskIds();
    // add the main task to receive from iteself
    int key = router.mainTaskOfExecutor(instancePlan.getThisExecutor(), MPIContext.DEFAULT_PATH);
    List<Integer> mainReceives = integerMapMap.get(key);
    if (mainReceives == null) {
      mainReceives = new ArrayList<>();
      integerMapMap.put(key, mainReceives);
    }
    if (key != destination) {
      mainReceives.add(key);
    }
    return integerMapMap;
  }

  @Override
  public void progress() {
    super.progress();

    finalReceiver.progress();
    partialReceiver.progress();
  }
}
