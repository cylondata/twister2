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

import java.util.ArrayList;
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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.channel.ChannelReceiver;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
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
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class TreeBroadcast implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(TreeBroadcast.class.getName());

  private int source;

  private Set<Integer> destinations;

  private BinaryTreeRouter router;

  private MessageReceiver finalReceiver;

  /**
   * Keep sources as a set to return
   */
  private Set<Integer> sourceSet;

  /**
   * Keep sources as a set to return
   */
  private Set<Integer> targetSet;

  /**
   * The sources which are pending for finish
   */
  private Set<Integer> pendingFinishSources;

  /**
   * The sources that are finished
   */
  private Set<Integer> finishedSources;

  /**
   * Sources of this worker
   */
  private Set<Integer> thisSources;

  /**
   * When we receive a message, we need to distribute it to these tasks running in the same worker
   */
  private List<Integer> receiveTasks = new ArrayList<>();

  private ChannelDataFlowOperation delegate;
  private MessageSchema messageSchema;
  private Config config;
  private LogicalPlan instancePlan;
  private int executor;
  private int edge;
  private MessageType dataType;
  private MessageType recvDataType;
  private Map<Integer, ArrayBlockingQueue<OutMessage>>
      pendingSendMessagesPerSource = new HashMap<>();
  private Lock lock = new ReentrantLock();

  private Map<Integer, RoutingParameters> routingParametersCache = new HashMap<>();

  /**
   * The key type
   */
  private MessageType keyType;
  private MessageType recvKeyType;

  public TreeBroadcast(TWSChannel channel, int src, Set<Integer> dests,
                       MessageReceiver finalRcvr, MessageType keyType,
                       MessageType dataType, MessageSchema messageSchema) {
    this.source = src;
    this.destinations = dests;
    this.finalReceiver = finalRcvr;
    this.keyType = keyType;
    this.dataType = dataType;

    this.delegate = new ChannelDataFlowOperation(channel);
    this.messageSchema = messageSchema;

    this.sourceSet = new HashSet<>();
    sourceSet.add(src);
    this.targetSet = new HashSet<>(dests);

    this.pendingFinishSources = new HashSet<>();
    this.finishedSources = new HashSet<>();
  }

  public TreeBroadcast(TWSChannel channel, int src, Set<Integer> dests,
                       MessageReceiver finalRcvr, MessageSchema messageSchema) {
    this.source = src;
    this.destinations = dests;
    this.finalReceiver = finalRcvr;

    this.delegate = new ChannelDataFlowOperation(channel);
    this.messageSchema = messageSchema;

    this.sourceSet = new HashSet<>();
    sourceSet.add(src);
    this.targetSet = new HashSet<>(dests);

    this.pendingFinishSources = new HashSet<>();
    this.finishedSources = new HashSet<>();
  }

  @Override
  public void close() {
    if (finalReceiver != null) {
      finalReceiver.close();
    }
    delegate.close();
  }

  @Override
  public void reset() {
    if (finalReceiver != null) {
      finalReceiver.clean();
    }
    pendingFinishSources.clear();
    finishedSources.clear();
  }

  @Override
  public void finish(int target) {
    if (!thisSources.contains(source)) {
      throw new RuntimeException("Invalid source completion: " + source);
    }
    lock.lock();
    try {
      pendingFinishSources.add(source);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return instancePlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(edge);
  }

  private Queue<Pair<MessageHeader, Object>> currentReceiveMessage;

  private int receiveIndex = 0;

  public boolean receiveMessage(MessageHeader h, Object o) {
    // we always receive to the main task
    return currentReceiveMessage.offer(new ImmutablePair<>(h, o));
  }

  private boolean receiveProgressMessage() {
    Pair<MessageHeader, Object> pair = currentReceiveMessage.peek();

    if (pair == null) {
      return false;
    }

    MessageHeader header = pair.getLeft();
    Object object = pair.getRight();

    boolean allSent = true;
    boolean done = true;
    for (int i = receiveIndex; i < receiveTasks.size(); i++) {
      if (!finalReceiver.onMessage(
          header.getSourceId(), CommunicationContext.DEFAULT_DESTINATION,
          receiveTasks.get(i), header.getFlags(), object)) {
        done = false;
        allSent = false;
        break;
      }
      receiveIndex++;
    }

    if (allSent) {
      currentReceiveMessage.poll();
      receiveIndex = 0;
    }

    return done;
  }

  /**
   * Initialize
   */
  public void init(Config cfg, MessageType dType,
                   MessageType recvDType,
                   MessageType kType,
                   MessageType recvKType,
                   LogicalPlan tPlan, int ed) {
    this.config = cfg;
    this.instancePlan = tPlan;

    this.dataType = dType;
    this.recvDataType = recvDType;

    this.keyType = kType;
    this.recvKeyType = recvKType;

    this.edge = ed;
    this.executor = tPlan.getThisWorker();
    this.currentReceiveMessage = new ArrayBlockingQueue<>(CommunicationContext.sendPendingMax(cfg));

    // we will only have one distinct route
    router = new BinaryTreeRouter(cfg, tPlan, source, destinations);

    if (this.finalReceiver != null) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    } else {
      throw new RuntimeException("Final receiver is required");
    }

    LOG.log(Level.FINE, String.format("%d bast sources %d dest %s send tasks: %s", executor,
        source, destinations, router.sendQueueIds()));
    thisSources = TaskPlanUtils.getTasksOfThisWorker(tPlan, sourceSet);

    Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<OutMessage> pendingSendMessages =
          new ArrayBlockingQueue<>(CommunicationContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, Serializers.get(kType != null, this.messageSchema));
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
      pendingReceiveMessagesPerSource.put(source, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(source, new ArrayBlockingQueue<>(capacity));
      deSerializerMap.put(source, Deserializers.get(kType != null, this.messageSchema));
    }

    calculateRoutingParameters();

    for (Integer s : srcs) {
      routingParametersCache.put(s, sendRoutingParameters(s, 0));
    }

    if (this.keyType != null) {
      delegate.init(cfg, dType, recvDataType, kType, recvKType, tPlan, ed,
          router.receivingExecutors(), this,
          pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
          pendingReceiveDeSerializations, serializerMap, deSerializerMap, true);
    } else {
      delegate.init(cfg, dType, recvDataType, tPlan, ed,
          router.receivingExecutors(), this,
          pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
          pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
    }
  }

  public void init(Config cfg, MessageType dType,
                   MessageType recvDType,
                   LogicalPlan tPlan, int ed) {
    this.init(cfg, dType, recvDType, this.keyType, this.keyType, tPlan, ed);
  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType dType, LogicalPlan tPlan, int ed) {
    this.init(cfg, dType, dType, tPlan, ed);
  }

  @Override
  public boolean sendPartial(int src, Object message, int flags) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  public boolean isComplete() {
    if (lock.tryLock()) {
      boolean done = delegate.isComplete();
      try {
        boolean complete = finalReceiver.isComplete();
        boolean needFinishProgress = handleFinish();
        return done && complete && !needFinishProgress;
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  @Override
  public boolean send(int src, Object message, int flags) {
    RoutingParameters routingParameters = sendRoutingParameters(src, 0);
    return delegate.sendMessage(src, message, 0, flags, routingParameters);
  }

  @Override
  public boolean send(int src, Object message, int flags, int target) {
    RoutingParameters routingParameters = sendRoutingParameters(src, 0);
    return delegate.sendMessage(src, message, target, flags, routingParameters);
  }

  private void calculateRoutingParameters() {
    RoutingParameters parameters = sendRoutingParameters(source, 0);
    routingParametersCache.put(source, parameters);

    Set<Integer> thisTargets = TaskPlanUtils.getTasksOfThisWorker(instancePlan, destinations);
    Set<Integer> thisWorkerTasks = TaskPlanUtils.getThisWorkerTasks(instancePlan);
    if (!thisWorkerTasks.contains(source)) {
      receiveTasks.addAll(thisTargets);
    }
  }

  @Override
  public boolean sendPartial(int src, Object message, int flags, int target) {
    return false;
  }

  @Override
  public boolean progress() {
    boolean partialNeedsProgress;
    boolean needFinishProgress;
    boolean needReceiveProgress;
    if (lock.tryLock()) {
      try {
        // lets send the finished one
        needFinishProgress = handleFinish();

        // send the messages to targets
        needReceiveProgress = receiveProgressMessage();

        delegate.progress();
        partialNeedsProgress = finalReceiver.progress();
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, "un-expected error", t);
        throw new RuntimeException(String.format("%d exception", executor), t);
      } finally {
        lock.unlock();
      }
      return partialNeedsProgress || needFinishProgress || needReceiveProgress;
    }
    return true;
  }

  private boolean handleFinish() {
    for (int src : pendingFinishSources) {
      if (!finishedSources.contains(src)) {
        if (send(src, new byte[1], MessageFlags.SYNC_EMPTY, 0)) {
          finishedSources.add(src);
        } else {
          // no point in going further
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isDelegateComplete() {
    return delegate.isComplete();
  }

  @Override
  public boolean handleReceivedChannelMessage(ChannelMessage currentMessage) {
    int src = router.mainTaskOfExecutor(instancePlan.getThisWorker(),
        CommunicationContext.DEFAULT_DESTINATION);

    RoutingParameters routingParameters;
    if (routingParametersCache.containsKey(src)) {
      routingParameters = routingParametersCache.get(src);
    } else {
      routingParameters = sendRoutingParameters(src, CommunicationContext.DEFAULT_DESTINATION);
    }

    ArrayBlockingQueue<OutMessage> pendingSendMessages = pendingSendMessagesPerSource.get(src);

    // create a send message to keep track of the serialization at the initial stage
    // the sub-edge is 0
    int di = -1;
    if (routingParameters.getExternalRoutes().size() > 0) {
      di = routingParameters.getDestinationId();
    }
    OutMessage sendMessage = new OutMessage(src,
        currentMessage.getHeader().getEdge(),
        di, CommunicationContext.DEFAULT_DESTINATION, currentMessage.getHeader().getFlags(),
        routingParameters.getInternalRoutes(),
        routingParameters.getExternalRoutes(), dataType, this.keyType, delegate,
        CommunicationContext.EMPTY_OBJECT);
    sendMessage.getChannelMessages().offer(currentMessage);

    // we need to update here
    if (!currentMessage.isOutCountUpdated()) {
      currentMessage.incrementRefCount(routingParameters.getExternalRoutes().size());
      currentMessage.setOutCountUpdated(true);
    }
    // this is a complete message
    sendMessage.setSendState(OutMessage.SendState.SERIALIZED);

    // now try to put this into pending
    return pendingSendMessages.offer(sendMessage);
  }

  private RoutingParameters sendRoutingParameters(int s, int path) {
    if (routingParametersCache.containsKey(s)) {
      return routingParametersCache.get(s);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();
      // get the expected routes
      Map<Integer, Set<Integer>> internalRouting = router.getInternalSendTasks(source);
      if (internalRouting == null) {
        throw new RuntimeException("Un-expected message from source: " + s);
      }

      Set<Integer> internalSourceRouting = internalRouting.get(s);
      if (internalSourceRouting != null) {
        // we always use path 0 because only one path
        routingParameters.addInternalRoutes(internalSourceRouting);
      }

      // get the expected routes
      Map<Integer, Set<Integer>> externalRouting = router.getExternalSendTasks(s);
      if (externalRouting == null) {
        throw new RuntimeException("Un-expected message from source: " + s);
      }
      Set<Integer> externalSourceRouting = externalRouting.get(s);
      if (externalSourceRouting != null) {
        routingParameters.addExternalRoutes(externalSourceRouting);
      }
      return routingParameters;
    }
  }

  @Override
  public boolean receiveSendInternally(int src, int target, int path, int flags, Object message) {
    return finalReceiver.onMessage(src, path, target, flags, message);
  }

  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }

  @Override
  public Set<Integer> getSources() {
    return sourceSet;
  }

  @Override
  public Set<Integer> getTargets() {
    return targetSet;
  }
}

