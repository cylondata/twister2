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
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;
import edu.iu.dsc.tws.comms.dfw.io.SingleMessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.SingleMessageSerializer;
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * A direct data flow operation sends peer to peer messages
 */
public class DataFlowDirect implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(
      DataFlowDirect.class.getName());
  private int sourceId;
  private int targetId;
  private DirectRouter router;
  private MessageReceiver finalReceiver;
  private ChannelDataFlowOperation delegete;
  private Config config;
  private TaskPlan instancePlan;
  private int executor;
  private Lock lock;

  public DataFlowDirect(TWSChannel channel,
                        int src, int target,
                        MessageReceiver finalRcvr) {
    this.sourceId = src;
    this.targetId = target;
    this.finalReceiver = finalRcvr;
    this.delegete = new ChannelDataFlowOperation(channel);
    this.lock = new ReentrantLock();
  }

  @Override
  public boolean receiveMessage(ChannelMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // check weather this message is for a sub task
    return finalReceiver.onMessage(header.getSourceId(), 0,
        targetId, header.getFlags(), object);
  }

  @Override
  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // we only have one destination in this case
    if (target != targetId) {
      throw new RuntimeException("We only have one destination");
    }

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
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    if (this.finalReceiver != null && isLastReceiver()) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, ChannelMessage>>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<ChannelMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> sources = new HashSet<>();
    sources.add(sourceId);
    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      pendingSendMessagesPerSource.put(s, new ArrayBlockingQueue<Pair<Object, OutMessage>>(
          DataFlowContext.sendPendingMax(cfg)));
      pendingReceiveDeSerializations.put(s, new ArrayBlockingQueue<ChannelMessage>(
          DataFlowContext.sendPendingMax(cfg)));
      serializerMap.put(s, new SingleMessageSerializer(new KryoSerializer()));
    }

    MessageDeSerializer messageDeSerializer = new SingleMessageDeSerializer(new KryoSerializer());
    deSerializerMap.put(targetId, messageDeSerializer);
    delegete.init(cfg, t, taskPlan, edge, router.receivingExecutors(),
        isLastReceiver(), this, pendingSendMessagesPerSource,
        pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("This method is not used by direct communication");
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, 0, flags, sendRoutingParameters(source, 0));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return delegete.sendMessage(source, message, target, flags,
        sendRoutingParameters(source, target));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return false;
  }

  @Override
  public boolean progress() {
    boolean partialNeedsProgress = false;
    boolean done;
    try {
      delegete.progress();
      done = delegete.isComplete();
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
    return partialNeedsProgress && done;
  }

  @Override
  public boolean isComplete() {
    if (lock.tryLock()) {
      boolean done = delegete.isComplete();
      try {
        boolean needsFurtherProgress = finalReceiver.progress();
        return done && !needsFurtherProgress;
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  @Override
  public void close() {
  }

  @Override
  public void finish(int target) {

  }

  @Override
  public TaskPlan getTaskPlan() {
    return null;
  }

  private boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
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
    routingParameters.setDestinationId(targetId);
    return routingParameters;
  }
}
