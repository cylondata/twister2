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
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * A direct data flow operation sends peer to peer messages, the messages are between one source
 * and another task
 */
public class OneToOne implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(OneToOne.class.getName());

  /**
   * The sources of the operation
   */
  private List<Integer> sources;

  /**
   * The targets of the operation
   */
  private List<Integer> targets;

  /**
   * Edge value to use
   */
  private int edgeValue;

  /**
   * Final receiver
   */
  private MessageReceiver finalReceiver;

  /**
   * The delegate
   */
  private ChannelDataFlowOperation delegate;
  private MessageSchema messageSchema;


  private Lock lock;

  /**
   * The task plan
   */
  private LogicalPlan logicalPlan;

  /**
   * The router to configure the routing
   */
  private DirectRouter router;

  /**
   * The routing parameters for the sources in this executor
   */
  private Map<Integer, RoutingParameters> routes = new HashMap<>();

  /**
   * Configuration
   */
  private Config config;

  /**
   * Message type
   */
  private MessageType type;

  /**
   * Keep sources as a set to return
   */
  private Set<Integer> sourceSet;

  /**
   * Keep targets as a set to return
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
   * Mapping between source and destinations
   */
  private Map<Integer, Integer> sourcesToDestinations = new HashMap<>();


  public OneToOne(TWSChannel channel,
                  List<Integer> src, List<Integer> target,
                  MessageReceiver finalRcvr, Config cfg, MessageType t,
                  LogicalPlan plan, int edge, MessageSchema messageSchema) {
    this.sources = src;
    this.targets = target;
    this.finalReceiver = finalRcvr;
    this.delegate = new ChannelDataFlowOperation(channel);
    this.messageSchema = messageSchema;
    this.lock = new ReentrantLock();
    this.edgeValue = edge;
    this.logicalPlan = plan;
    this.router = new DirectRouter(plan, sources, targets);
    this.config = cfg;
    this.type = t;
    this.sourceSet = new HashSet<>(sources);
    this.targetSet = new HashSet<>(target);
    this.pendingFinishSources = new HashSet<>();
    this.finishedSources = new HashSet<>();
    // the sources to targets mapping
    for (int i = 0; i < src.size(); i++) {
      sourcesToDestinations.put(sources.get(i), target.get(i));
    }
    init();
  }

  @Override
  public boolean receiveMessage(MessageHeader header, Object object) {
    int target = header.getDestinationIdentifier();
    // check weather this message is for a sub task
    return finalReceiver.onMessage(header.getSourceId(), 0,
        target, header.getFlags(), object);
  }

  @Override
  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // okay this must be for the
    return finalReceiver.onMessage(source, path, target, flags, message);
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return this.router.receiveExpectedTaskIds();
  }

  /**
   * Initialize
   */
  private void init() {
    Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    thisSources = TaskPlanUtils.getTasksOfThisWorker(logicalPlan, sourceSet);
    Set<Integer> thisTargets = TaskPlanUtils.getTasksOfThisWorker(
        logicalPlan, new HashSet<>(targets));
    for (int s : thisSources) {
      // later look at how not to allocate pairs for this each time
      pendingSendMessagesPerSource.put(s, new ArrayBlockingQueue<>(
          CommunicationContext.sendPendingMax(config)));
      serializerMap.put(s, Serializers.get(false, this.messageSchema));
    }

    for (int tar : thisTargets) {
      pendingReceiveDeSerializations.put(sources.get(targets.indexOf(tar)),
          new ArrayBlockingQueue<>(CommunicationContext.sendPendingMax(config)));

      pendingReceiveMessagesPerSource.put(sources.get(targets.indexOf(tar)),
          new ArrayBlockingQueue<>(CommunicationContext.sendPendingMax(config)));

      deSerializerMap.put(tar, Deserializers.get(false, this.messageSchema));
    }

    // calculate the routing parameters
    calculateRoutingParameters();

    // initialize the final receiver
    this.finalReceiver.init(config, this, receiveExpectedTaskIds());

    delegate.init(config, type, logicalPlan, edgeValue, router.receivingExecutors(),
        this, pendingSendMessagesPerSource,
        pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("This method is not used by direct communication");
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegate.sendMessage(source, message, 0, flags, routes.get(source));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return delegate.sendMessage(source, message, target, flags, routes.get(source));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return false;
  }

  @Override
  public boolean progress() {
    boolean partialNeedsProgress = false;
    boolean needFinishProgress = true;
    boolean done;
    try {
      // lets send the finished one
      if (lock.tryLock()) {
        try {
          needFinishProgress = handleFinish();
        } finally {
          lock.unlock();
        }
      }

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
      throw new RuntimeException(t);
    }
    return partialNeedsProgress || !done || needFinishProgress;
  }

  private boolean handleFinish() {
    for (int source : pendingFinishSources) {
      if (!finishedSources.contains(source)) {
        int dest = sourcesToDestinations.get(source);
        if (send(source, new byte[1], MessageFlags.SYNC_EMPTY, dest)) {
          finishedSources.add(source);
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
      finishedSources.clear();
      pendingFinishSources.clear();
    }
  }

  @Override
  public void finish(int source) {
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
    return logicalPlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(edgeValue);
  }

  private void calculateRoutingParameters() {
    Set<Integer> workerTasks = logicalPlan.getLogicalIdsOfThisWorker();
    for (int i = 0; i < sources.size(); i++) {
      // for each source we have a fixed target
      int src = sources.get(i);
      int tar = targets.get(i);

      if (workerTasks != null && workerTasks.contains(src)) {
        RoutingParameters params = sendRoutingParameters(src, tar);
        routes.put(src, params);
      }
    }
  }

  private RoutingParameters sendRoutingParameters(int source, int target) {
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
    routingParameters.setDestinationId(target);
    return routingParameters;
  }

  @Override
  public MessageType getDataType() {
    return type;
  }

  public Set<Integer> getSources() {
    return sourceSet;
  }

  @Override
  public Set<Integer> getTargets() {
    return targetSet;
  }
}
