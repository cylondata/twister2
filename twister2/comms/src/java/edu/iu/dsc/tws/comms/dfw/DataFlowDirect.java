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
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * A direct data flow operation sends peer to peer messages, the messages are between one source
 * and another task
 */
public class DataFlowDirect implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(DataFlowDirect.class.getName());

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


  private Lock lock;

  /**
   * The task plan
   */
  private TaskPlan taskPlan;

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

  public DataFlowDirect(TWSChannel channel,
                        List<Integer> src, List<Integer> target,
                        MessageReceiver finalRcvr, Config cfg, MessageType t,
                        TaskPlan plan, int edge) {
    this.sources = src;
    this.targets = target;
    this.finalReceiver = finalRcvr;
    this.delegate = new ChannelDataFlowOperation(channel);
    this.lock = new ReentrantLock();
    this.edgeValue = edge;
    this.taskPlan = plan;
    this.router = new DirectRouter(plan, sources, targets);
    this.config = cfg;
    this.type = t;
    this.sourceSet = new HashSet<>(sources);
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

  @Override
  public boolean passMessageDownstream(Object object, ChannelMessage currentMessage) {
    return false;
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return this.router.receiveExpectedTaskIds();
  }

  /**
   * Initialize
   */
  private void init() {
    Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, InMessage>>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    thisSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sourceSet);
    for (int s : thisSources) {
      // later look at how not to allocate pairs for this each time
      pendingSendMessagesPerSource.put(s, new ArrayBlockingQueue<>(
          DataFlowContext.sendPendingMax(config)));
      pendingReceiveDeSerializations.put(s, new ArrayBlockingQueue<>(
          DataFlowContext.sendPendingMax(config)));
      serializerMap.put(s, new UnifiedSerializer(new KryoSerializer(),
          taskPlan.getThisExecutor(), type));
    }

    for (int tar : targets) {
      MessageDeSerializer messageDeSerializer = new UnifiedDeserializer(new KryoSerializer(),
          taskPlan.getThisExecutor(), type);
      deSerializerMap.put(tar, messageDeSerializer);
    }

    // calculate the routing parameters
    calculateRoutingParameters();

    // initialize the final receiver
    this.finalReceiver.init(config, this, receiveExpectedTaskIds());

    delegate.init(config, type, taskPlan, edgeValue, router.receivingExecutors(),
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
    boolean needFinishProgress;
    boolean done;
    try {
      // lets send the finished one
      needFinishProgress = handleFinish();

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
        if (send(source, new byte[1], MessageFlags.END, dest)) {
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
  public void clean() {
    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public void finish(int source) {
    if (!thisSources.contains(source)) {
      throw new RuntimeException("Invalid source completion: " + source);
    }
    pendingFinishSources.add(source);
  }

  @Override
  public TaskPlan getTaskPlan() {
    return taskPlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(edgeValue);
  }

  private void calculateRoutingParameters() {
    Set<Integer> workerTasks = taskPlan.getTasksOfThisExecutor();
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

  public Set<Integer> getSources() {
    return sourceSet;
  }
}
