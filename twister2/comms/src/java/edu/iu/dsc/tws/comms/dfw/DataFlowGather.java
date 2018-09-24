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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MultiMessageDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.MultiMessageSerializer;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class DataFlowGather implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(DataFlowGather.class.getName());

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  protected int destination;

  // the router to calculate the tasks
  private InvertedBinaryTreeRouter router;

  private MessageReceiver finalReceiver;

  private MessageReceiver partialReceiver;

  private int index = 0;

  private int pathToUse = DataFlowContext.DEFAULT_DESTINATION;

  private ChannelDataFlowOperation delegete;
  private TaskPlan instancePlan;
  private int executor;
  private MessageType dataType;
  private MessageType keyType;
  private MessageType rcvDataType;
  private boolean isKeyed;
  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();
  private Table<Integer, Integer, RoutingParameters> partialRoutingParamCache
      = HashBasedTable.create();
  private Lock lock = new ReentrantLock();
  private Lock partialLock = new ReentrantLock();
  private int edge;

  public DataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                        MessageReceiver finalRcvr,
                        int indx, int p,
                        Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this(channel, sources, destination, finalRcvr, new GatherStreamingPartialReceiver(),
        indx, p, cfg, t, taskPlan, edge);
  }

  public DataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                        MessageReceiver finalRcvr,
                        MessageReceiver partialRcvr, int indx, int p,
                        Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this(channel, sources, destination, finalRcvr, partialRcvr,
        indx, p, cfg, taskPlan, false, t, t, null, edge);
    this.isKeyed = false;
  }

  public DataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                        MessageReceiver finalRcvr,
                        MessageReceiver partialRcvr, int indx, int p,
                        Config cfg, TaskPlan taskPlan, boolean keyed, MessageType dType,
                        MessageType rcvDType, MessageType kType, int edge) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.keyType = kType;
    this.dataType = dType;
    this.instancePlan = taskPlan;
    this.isKeyed = keyed;
    this.rcvDataType = rcvDType;
    this.edge = edge;
    this.delegete = new ChannelDataFlowOperation(channel);
  }

  protected boolean isLast() {
    return router.isLastReceiver();
  }

  /**
   * We can receive messages from internal tasks or an external task, we allways receive messages
   * to the main task of the executor and we go from there
   */
  @Override
  public boolean receiveMessage(ChannelMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
    // check weather this message is for a sub task
    if (!isLast()
        && partialReceiver != null) {
      return partialReceiver.onMessage(header.getSourceId(),
          DataFlowContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              DataFlowContext.DEFAULT_DESTINATION), header.getFlags(), object);
    } else {
      return finalReceiver.onMessage(header.getSourceId(),
          DataFlowContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              DataFlowContext.DEFAULT_DESTINATION), header.getFlags(), object);
    }
  }

  private RoutingParameters partialSendRoutingParameters(int source, int path) {
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

  private RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();

    // get the expected routes
    Map<Integer, Set<Integer>> internalRouting = router.getInternalSendTasks(source);
    if (internalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    // we are going to add source if we are the main executor
    if (router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
        DataFlowContext.DEFAULT_DESTINATION) == source) {
      routingParameters.addInteranlRoute(source);
    }

    // we should not have the route for main task to outside at this point
    Set<Integer> sourceInternalRouting = internalRouting.get(source);
    if (sourceInternalRouting != null) {
      routingParameters.addInternalRoutes(sourceInternalRouting);
    }
    try {
      routingParameters.setDestinationId(router.destinationIdentifier(source, path));
    } catch (RuntimeException e) {
      LOG.info(String.format("%d exception %d %d %d %s",
          executor, index, pathToUse, destination, router.getDestinationIdentifiers()));
    }
    return routingParameters;
  }

  private boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
      return finalReceiver.onMessage(source, path, target, flags, message);
    } else {
      // now we need to serialize this to the buffer
      return partialReceiver.onMessage(source, path, target, flags, message);
    }
  }

  @Override
  public boolean passMessageDownstream(Object object, ChannelMessage currentMessage) {
    return true;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, pathToUse, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return delegete.sendMessage(source, message, target, flags,
        sendRoutingParameters(source, target));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return delegete.sendMessagePartial(source, message, target, flags,
        partialSendRoutingParameters(source, target));
  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int ed) {
    this.dataType = t;
    this.instancePlan = taskPlan;
    this.executor = taskPlan.getThisExecutor();
    this.edge = ed;
    // we only have one path
    this.router = new InvertedBinaryTreeRouter(cfg, taskPlan,
        destination, sources, index);

    // initialize the receive
    if (this.partialReceiver != null && !isLastReceiver()) {
      partialReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

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

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, OutMessage>>(
              DataFlowContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new MultiMessageSerializer(new KryoSerializer(), executor));
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
          new ArrayBlockingQueue<Pair<Object, ChannelMessage>>(capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<ChannelMessage>(capacity));
      deSerializerMap.put(e, new MultiMessageDeserializer(new KryoSerializer(), executor));
    }

    Set<Integer> sourcesOfThisExec = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    for (int s : sourcesOfThisExec) {
      sendRoutingParameters(s, pathToUse);
      partialSendRoutingParameters(s, pathToUse);
    }

    delegete.init(cfg, t, rcvDataType, keyType, keyType, taskPlan, ed,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    return delegete.sendMessagePartial(source, message, pathToUse, flags,
        partialSendRoutingParameters(source, pathToUse));
  }

  public boolean isComplete() {
    boolean done = delegete.isComplete();
    boolean needsFurtherProgress = OperationUtils.progressReceivers(delegete, lock, finalReceiver,
        partialLock, partialReceiver);
    return done && !needsFurtherProgress;
  }

  @Override
  public boolean isDelegeteComplete() {
    return delegete.isComplete();
  }

  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return OperationUtils.getIntegerListMap(router, instancePlan, destination);
  }

  @Override
  public boolean progress() {
    return OperationUtils.progressReceivers(delegete, lock, finalReceiver,
        partialLock, partialReceiver);
  }


  @Override
  public void close() {

  }

  @Override
  public void finish(int source) {
    while (!send(source, new double[0], MessageFlags.END)) {
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
  public TaskPlan getTaskPlan() {
    return instancePlan;
  }

  public int getEdge() {
    return edge;
  }
}
