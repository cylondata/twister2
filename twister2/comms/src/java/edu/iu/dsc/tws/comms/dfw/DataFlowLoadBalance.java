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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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
import edu.iu.dsc.tws.comms.routing.PartitionRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class DataFlowLoadBalance implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(DataFlowLoadBalance.class.getName());

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private PartitionRouter router;

  private Map<Integer, Integer> destinationIndex;
  private Set<Integer> thisSources;
  private Destinations dests = new Destinations();
  private List<Integer> destinationsList;
  private Set<Integer> thisTasks;
  private MessageReceiver finalReceiver;

  private ChannelDataFlowOperation delegete;
  private Config config;
  private TaskPlan instancePlan;
  private int executor;
  private MessageType type;
  private AtomicBoolean finalReceiverProgress;

  /**
   * A place holder for keeping the internal and external destinations
   */
  @SuppressWarnings("VisibilityModifier")
  private class Destinations {
    List<Integer> internal = new ArrayList<>();
    List<Integer> external = new ArrayList<>();
  }

  public DataFlowLoadBalance(TWSChannel channel, Set<Integer> srcs,
                             Set<Integer> dests, MessageReceiver finalRcvr) {
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);
    this.delegete = new ChannelDataFlowOperation(channel);

    for (int s : sources) {
      destinationIndex.put(s, 0);
    }

    this.finalReceiver = finalRcvr;
    this.finalReceiverProgress = new AtomicBoolean(false);
  }

  protected void setupRouting() {

  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this.thisSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    LOG.info(String.format("%d setup loadbalance routing %s",
        taskPlan.getThisExecutor(), thisSources));
    this.thisTasks = taskPlan.getTasksOfThisExecutor();
    this.router = new PartitionRouter(taskPlan, sources, destinations);
    Map<Integer, Set<Integer>> internal = router.getInternalSendTasks(0);
    Map<Integer, Set<Integer>> external = router.getExternalSendTasks(0);
    this.instancePlan = taskPlan;
    this.type = t;

    LOG.info(String.format("%d adding internal/external routing", taskPlan.getThisExecutor()));
    try {
      for (int s : thisSources) {
        Set<Integer> integerSetMap = internal.get(s);
        if (integerSetMap != null) {
          this.dests.internal.addAll(integerSetMap);
        }

        Set<Integer> integerSetMap1 = external.get(s);
        if (integerSetMap1 != null) {
          this.dests.external.addAll(integerSetMap1);
        }
        LOG.info(String.format("%d adding internal/external routing %d",
            taskPlan.getThisExecutor(), s));
        break;
      }
    } catch (Throwable te) {
      te.printStackTrace();
    }
    LOG.info(String.format("%d done adding internal/external routing",
        taskPlan.getThisExecutor()));

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

    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, OutMessage>>(
              DataFlowContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new SingleMessageSerializer(new KryoSerializer()));
    }

    int maxReceiveBuffers = DataFlowContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<Pair<Object, ChannelMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<Pair<Object, ChannelMessage>>(
              capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<ChannelMessage>(capacity));
      deSerializerMap.put(e, new SingleMessageDeSerializer(new KryoSerializer()));
    }

    delegete.init(cfg, t, taskPlan, edge,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("Not supported method");
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
    throw new RuntimeException("Not supported method");
  }

  @Override
  public boolean progress() {
    try {
      delegete.progress();
      if (finalReceiverProgress.compareAndSet(false, true)) {
        finalReceiver.progress();
        finalReceiverProgress.compareAndSet(true, false);
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
    return true;
  }

  @Override
  public void close() {
  }

  @Override
  public void finish(int source) {
  }

  @Override
  public TaskPlan getTaskPlan() {
    return instancePlan;
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    int destination = 0;

    routingParameters.setDestinationId(destination);

    if (!destinationIndex.containsKey(source)) {
      throw new RuntimeException(String.format(
          "Un-expected source %d in loadbalance executor %d %s", source,
          executor, destinationIndex));
    }

    int index = destinationIndex.get(source);
    int route = destinationsList.get(index);

    if (thisTasks.contains(route)) {
      routingParameters.addInteranlRoute(route);
    } else {
      routingParameters.addExternalRoute(route);
    }
    routingParameters.setDestinationId(route);
//    LOG.info(String.format("%d Eending to %d: %d %s",
//        instancePlan.getThisExecutor(), index, route, destinationIndex));

    index = (index + 1) % destinations.size();
    destinationIndex.put(source, index);
//    LOG.info(String.format("%d EEending to %d: %d %s",
//        instancePlan.getThisExecutor(), index, route, destinationIndex));
    return routingParameters;
  }

  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // okay this must be for the
    return finalReceiver.onMessage(source, path, target, flags, message);
  }

  @Override
  public boolean passMessageDownstream(Object object, ChannelMessage currentMessage) {
    return true;
  }

  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }

  protected boolean isLast(int source, int path, int taskIdentifier) {
    return destinations.contains(taskIdentifier);
  }

  public boolean receiveMessage(ChannelMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    return finalReceiver.onMessage(header.getSourceId(), DataFlowContext.DEFAULT_DESTINATION,
        router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
            DataFlowContext.DEFAULT_DESTINATION), header.getFlags(), object);
  }

  protected boolean isLastReceiver() {
    return true;
  }
}
