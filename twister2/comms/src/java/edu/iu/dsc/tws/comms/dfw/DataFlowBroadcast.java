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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;
import edu.iu.dsc.tws.comms.dfw.io.UnifiedDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.UnifiedSerializer;
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class DataFlowBroadcast implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(DataFlowBroadcast.class.getName());

  private int source;

  private Set<Integer> destinations;

  private BinaryTreeRouter router;

  private MessageReceiver finalReceiver;

  /**
   * Keep sources as a set to return
   */
  private Set<Integer> sourceSet;

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
  private Config config;
  private TaskPlan instancePlan;
  private int executor;
  private int edge;
  private MessageType type;
  private Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>>
      pendingSendMessagesPerSource = new HashMap<>();
  private Lock lock = new ReentrantLock();

  private Map<Integer, RoutingParameters> routingParametersCache = new HashMap<>();

  public DataFlowBroadcast(TWSChannel channel, int src, Set<Integer> dests,
                           MessageReceiver finalRcvr) {
    this.source = src;
    this.destinations = dests;
    this.finalReceiver = finalRcvr;

    this.delegate = new ChannelDataFlowOperation(channel);

    this.sourceSet = new HashSet<>();
    sourceSet.add(src);

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
  public void clean() {
    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public void finish(int target) {
    if (!thisSources.contains(source)) {
      throw new RuntimeException("Invalid source completion: " + source);
    }
    pendingFinishSources.add(source);
  }

  @Override
  public TaskPlan getTaskPlan() {
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
          header.getSourceId(), DataFlowContext.DEFAULT_DESTINATION,
          receiveTasks.get(receiveIndex), header.getFlags(), object)) {
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
   * @param cfg
   * @param t
   * @param tPlan
   * @param ed
   */
  public void init(Config cfg, MessageType t, TaskPlan tPlan, int ed) {
    this.config = cfg;
    this.instancePlan = tPlan;
    this.type = t;
    this.edge = ed;
    this.executor = tPlan.getThisExecutor();
    this.currentReceiveMessage = new ArrayBlockingQueue<>(DataFlowContext.sendPendingMax(cfg));

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

    Map<Integer, Queue<Pair<Object, InMessage>>> pendingReceiveMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, OutMessage>>(
              DataFlowContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new UnifiedSerializer(new KryoSerializer(), executor, type));
    }

    int maxReceiveBuffers = DataFlowContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<Pair<Object, InMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<>(
              capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<>(capacity));
      deSerializerMap.put(e, new UnifiedDeserializer(new KryoSerializer(), executor, type));
    }

    calculateRoutingParameters();

    for (Integer s : srcs) {
      routingParametersCache.put(s, sendRoutingParameters(s, 0));
    }

    delegate.init(cfg, t, tPlan, ed,
        router.receivingExecutors(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
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
        boolean needsFurtherProgress = finalReceiver.progress();
        boolean needFinishProgress = handleFinish();
        return done && !needsFurtherProgress && !needFinishProgress;
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
    Set<Integer> workerTasks = instancePlan.getTasksOfThisExecutor();
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
    boolean partialNeedsProgress = false;
    boolean needFinishProgress;
    boolean done;
    boolean needReceiveProgress;
    try {
      // lets send the finished one
      needFinishProgress = handleFinish();

      // send the messages to targets
      needReceiveProgress = receiveProgressMessage();

      delegate.progress();
      done = delegate.isComplete();
      if (lock.tryLock()) {
        try {
          partialNeedsProgress = finalReceiver.progress();
        } finally {
          lock.unlock();
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(String.format("%d exception", executor), t);
    }
    return partialNeedsProgress || !done || needFinishProgress || needReceiveProgress;
  }

  private boolean handleFinish() {
    for (int src : pendingFinishSources) {
      if (!finishedSources.contains(src)) {
        if (send(src, new byte[1], MessageFlags.END, 0)) {
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

  public boolean handleReceivedChannelMessage(ChannelMessage currentMessage) {
    int src = router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
        DataFlowContext.DEFAULT_DESTINATION);

    RoutingParameters routingParameters;
    if (routingParametersCache.containsKey(src)) {
      routingParameters = routingParametersCache.get(src);
    } else {
      routingParameters = sendRoutingParameters(src, DataFlowContext.DEFAULT_DESTINATION);
    }

    ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(src);

    // create a send message to keep track of the serialization at the initial stage
    // the sub-edge is 0
    int di = -1;
    if (routingParameters.getExternalRoutes().size() > 0) {
      di = routingParameters.getDestinationId();
    }
    OutMessage sendMessage = new OutMessage(src,
        currentMessage.getHeader().getEdge(),
        di, DataFlowContext.DEFAULT_DESTINATION, currentMessage.getHeader().getFlags(),
        routingParameters.getInternalRoutes(),
        routingParameters.getExternalRoutes(), type, null, delegate);
    sendMessage.getChannelMessages().offer(currentMessage);

    // we need to update here
    if (!currentMessage.isOutCountUpdated()) {
      currentMessage.incrementRefCount(routingParameters.getExternalRoutes().size());
      currentMessage.setOutCountUpdated(true);
    }
    // this is a complete message
    sendMessage.setSendState(OutMessage.SendState.SERIALIZED);

    // now try to put this into pending
    return pendingSendMessages.offer(
        new ImmutablePair<>(DataFlowContext.EMPTY_OBJECT, sendMessage));
  }

  public boolean passMessageDownstream(Object object, ChannelMessage currentMessage) {
    int src = router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
        DataFlowContext.DEFAULT_DESTINATION);

    RoutingParameters routingParameters;
    if (routingParametersCache.containsKey(src)) {
      routingParameters = routingParametersCache.get(src);
    } else {
      routingParameters = sendRoutingParameters(src, DataFlowContext.DEFAULT_DESTINATION);
    }

    ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
        pendingSendMessagesPerSource.get(src);

    // create a send message to keep track of the serialization
    // at the intial stage the sub-edge is 0
    int di = -1;
    if (routingParameters.getExternalRoutes().size() > 0) {
      di = routingParameters.getDestinationId();
    }
    OutMessage sendMessage = new OutMessage(src,
        currentMessage.getHeader().getEdge(),
        di, DataFlowContext.DEFAULT_DESTINATION, currentMessage.getHeader().getFlags(),
        routingParameters.getInternalRoutes(),
        routingParameters.getExternalRoutes(), type, null, delegate);
    sendMessage.getChannelMessages().offer(currentMessage);
    // this is a complete message
    sendMessage.setSendState(OutMessage.SendState.SERIALIZED);

    // now try to put this into pending
    return pendingSendMessages.offer(
        new ImmutablePair<>(object, sendMessage));
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
}

