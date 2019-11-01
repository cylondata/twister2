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
package edu.iu.dsc.tws.comms.dfw;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.channel.ChannelReceiver;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageDeSerializer;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.packing.MessageSerializer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Deserializers;
import edu.iu.dsc.tws.comms.dfw.io.Serializers;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MToOneTree implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(MToOneTree.class.getName());

  /**
   * the source tasks
   */
  protected Set<Integer> sources;

  /**
   * The targets
   */
  private Set<Integer> targets;

  /**
   * The target tast
   */
  protected int destination;

  /**
   * The edge to be used
   */
  private int edgeValue;

  /**
   * The router
   */
  private InvertedBinaryTreeRouter router;

  /**
   * Final receiver
   */
  private MessageReceiver finalReceiver;

  /**
   * The local receiver
   */
  private MessageReceiver partialReceiver;

  private int index;

  private int pathToUse = CommunicationContext.DEFAULT_DESTINATION;

  private ChannelDataFlowOperation delegete;
  private MessageSchema messageSchema;
  private LogicalPlan instancePlan;
  private MessageType dataType;
  private MessageType keyType;

  private boolean isKeyed = false;

  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();
  private Table<Integer, Integer, RoutingParameters> partialRoutingParamCache
      = HashBasedTable.create();
  private Lock lock = new ReentrantLock();
  private Lock partialLock = new ReentrantLock();

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr,
                    MessageReceiver partialRcvr, int indx, int p,
                    MessageSchema messageSchema) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.delegete = new ChannelDataFlowOperation(channel);
    this.messageSchema = messageSchema;
    this.targets = new HashSet<>();
    this.targets.add(destination);
  }

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr,
                    MessageReceiver partialRcvr, int indx, int p, boolean keyed,
                    MessageType kType, MessageType dType,
                    MessageSchema messageSchema) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.delegete = new ChannelDataFlowOperation(channel);
    this.isKeyed = keyed;
    this.keyType = kType;
    this.dataType = dType;
    this.messageSchema = messageSchema;
    this.targets = new HashSet<>();
    this.targets.add(destination);
  }

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr, MessageReceiver partialRcvr,
                    MessageSchema messageSchema) {
    this(channel, sources, destination, finalRcvr, partialRcvr, 0, 0, messageSchema);
  }


  /**
   * We can receive messages from internal tasks or an external task, we allways receive messages
   * to the main task of the workerId and we go from there
   */
  public boolean receiveMessage(MessageHeader header, Object object) {
    // we always receive to the main task
    // check weather this message is for a sub task
    if (!router.isLastReceiver()
        && partialReceiver != null) {
      return partialReceiver.onMessage(header.getSourceId(),
          CommunicationContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisWorker(),
              CommunicationContext.DEFAULT_DESTINATION), header.getFlags(), object);
    } else {
      return finalReceiver.onMessage(header.getSourceId(), CommunicationContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisWorker(),
              CommunicationContext.DEFAULT_DESTINATION), header.getFlags(), object);
    }
  }

  private RoutingParameters partialSendRoutingParameters(int source, int path) {
    if (partialRoutingParamCache.contains(source, path)) {
      return partialRoutingParamCache.get(source, path);
    } else {
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
      partialRoutingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
    if (routingParamCache.contains(source, path)) {
      return routingParamCache.get(source, path);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();

      // get the expected routes
      Map<Integer, Set<Integer>> internalRouting = router.getInternalSendTasks(source);
      if (internalRouting == null) {
        throw new RuntimeException("Un-expected message from source: " + source);
      }

      // we are going to add source if we are the main workerId
      if (router.mainTaskOfExecutor(instancePlan.getThisWorker(),
          CommunicationContext.DEFAULT_DESTINATION) == source) {
        routingParameters.addInteranlRoute(source);
      }

      // we should not have the route for main task to outside at this point
      Set<Integer> sourceInternalRouting = internalRouting.get(source);
      if (sourceInternalRouting != null) {
        routingParameters.addInternalRoutes(sourceInternalRouting);
      }

      routingParameters.setDestinationId(router.destinationIdentifier(source, path));
      routingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
      return finalReceiver.onMessage(source, path, target, flags, message);
    } else {
      return partialReceiver.onMessage(source, path, target, flags, message);
    }
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, pathToUse, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return delegete.sendMessage(source, message, target, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return delegete.sendMessagePartial(source, message, target, flags,
        partialSendRoutingParameters(source, target));
  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, LogicalPlan logicalPlan, int edge) {
    this.instancePlan = logicalPlan;
    this.dataType = t;
    int workerId = instancePlan.getThisWorker();
    this.edgeValue = edge;

    // we only have one path
    this.router = new InvertedBinaryTreeRouter(cfg, logicalPlan,
        destination, sources, index);

    // initialize the receive
    if (this.partialReceiver != null && !router.isLastReceiver()) {
      partialReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    if (this.finalReceiver != null && router.isLastReceiver()) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    LOG.log(Level.FINE, String.format("%d reduce sources %s dest %d send tasks: %s",
        workerId, sources, destination, router.sendQueueIds()));

    Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<OutMessage> pendingSendMessages =
          new ArrayBlockingQueue<>(CommunicationContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, Serializers.get(isKeyed, this.messageSchema));
    }

    int maxReceiveBuffers = CommunicationContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    Set<Integer> execs = router.getReceiveSources();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<InMessage> pendingReceiveMessages = new ArrayBlockingQueue<>(capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<>(capacity));
      deSerializerMap.put(e, Deserializers.get(isKeyed, this.messageSchema));
    }

    Set<Integer> sourcesOfThisExec = TaskPlanUtils.getTasksOfThisWorker(logicalPlan, sources);
    for (int s : sourcesOfThisExec) {
      sendRoutingParameters(s, pathToUse);
      partialSendRoutingParameters(s, pathToUse);
    }

    delegete.init(cfg, t, t, keyType, keyType, logicalPlan, edge,
        router.receivingExecutors(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    return delegete.sendMessagePartial(source, message, pathToUse, flags,
        partialSendRoutingParameters(source, pathToUse));
  }

  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return OperationUtils.getIntegerListMap(router, instancePlan, destination);
  }

  @Override
  public boolean isDelegateComplete() {
    return delegete.isComplete();
  }

  @Override
  public boolean isComplete() {
    boolean done = delegete.isComplete();
    boolean complete = OperationUtils.areReceiversComplete(lock, finalReceiver,
        partialLock, partialReceiver);
    return done && complete;
  }

  @Override
  public boolean progress() {
    return OperationUtils.progressReceivers(delegete, lock,
        finalReceiver, partialLock, partialReceiver);
  }

  @Override
  public void close() {
    if (finalReceiver != null) {
      finalReceiver.close();
    }

    if (partialReceiver != null) {
      partialReceiver.close();
    }

    delegete.close();
  }

  @Override
  public void reset() {
    if (partialReceiver != null) {
      partialReceiver.clean();
    }

    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public void finish(int source) {
    while (!send(source, new byte[0], MessageFlags.SYNC_EMPTY)) {
      // lets progress until finish
      progress();
    }
  }

  @Override
  public MessageType getKeyType() {
    return keyType;
  }

  @Override
  public MessageType getDataType() {
    return dataType;
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return instancePlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(edgeValue);
  }

  @Override
  public Set<Integer> getSources() {
    return sources;
  }

  @Override
  public Set<Integer> getTargets() {
    return targets;
  }
}
