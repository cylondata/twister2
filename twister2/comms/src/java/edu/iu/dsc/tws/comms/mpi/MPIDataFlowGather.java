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
import java.util.HashMap;
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

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.MPIMultiMessageDeserializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMultiMessageSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageSerializer;
import edu.iu.dsc.tws.comms.mpi.io.gather.StreamingPartialGatherReceiver;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MPIDataFlowGather implements DataFlowOperation, MPIMessageReceiver {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowGather.class.getName());

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  protected int destination;

  private InvertedBinaryTreeRouter router;

  private MessageReceiver finalReceiver;

  private MessageReceiver partialReceiver;

  private int index = 0;

  private int pathToUse = MPIContext.DEFAULT_PATH;

  private MPIDataFlowOperation delegete;
  private TaskPlan instancePlan;
  private int executor;
  private MessageType type;
  private MessageType keyType;
  private boolean isKeyed;
  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();
  private Table<Integer, Integer, RoutingParameters> partialRoutingParamCache
      = HashBasedTable.create();
  private Lock lock = new ReentrantLock();
  private Lock partialLock = new ReentrantLock();

  public MPIDataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr,
                           int indx, int p,
                           Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this(channel, sources, destination, finalRcvr, new StreamingPartialGatherReceiver(),
        indx, p, cfg, t, taskPlan, edge);
  }

  public MPIDataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr,
                           int indx, int p,
                           Config cfg, MessageType t, MessageType keyType,
                           TaskPlan taskPlan, int edge) {
    this(channel, sources, destination, finalRcvr, new StreamingPartialGatherReceiver(),
        indx, p, cfg, t, keyType, taskPlan, edge);
    this.isKeyed = true;
  }

  public MPIDataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr, int indx, int p,
                           Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this(channel, sources, destination, finalRcvr, partialRcvr,
        indx, p, cfg, t, MessageType.SHORT, taskPlan, edge);
    this.isKeyed = false;
  }

  public MPIDataFlowGather(TWSChannel channel, Set<Integer> sources, int destination,
                           MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr, int indx, int p,
                           Config cfg, MessageType t, MessageType kt, TaskPlan taskPlan, int edge) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.keyType = kt;
    this.instancePlan = taskPlan;
    this.isKeyed = true;

    this.delegete = new MPIDataFlowOperation(channel);
  }

  protected boolean isLast() {
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
  public boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
    // check weather this message is for a sub task
    if (!isLast()
        && partialReceiver != null) {
//      LOG.info(String.format("%d calling PARTIAL receiver %d", executor, header.getSourceId()));
      return partialReceiver.onMessage(header.getSourceId(),
          MPIContext.DEFAULT_PATH,
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              MPIContext.DEFAULT_PATH), header.getFlags(), currentMessage);
    } else {
//      LOG.info(String.format("%d calling FINAL receiver %d", executor, header.getSourceId()));
      return finalReceiver.onMessage(header.getSourceId(),
          MPIContext.DEFAULT_PATH, router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              MPIContext.DEFAULT_PATH), header.getFlags(), object);
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
        MPIContext.DEFAULT_PATH) == source) {
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

  public boolean receiveSendInternally(int source, int t, int path, int flags, Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
//      LOG.info(String.format("%d internally FINAL receiver %d %s", executor, source,
//          finalReceiver.getClass().getName()));
      return finalReceiver.onMessage(source, path, t, flags, message);
    } else {
//      LOG.info(String.format("%d internally PARTIAL receiver %d %s", executor, source,
//          partialReceiver.getClass().getName()));
      // now we need to serialize this to the buffer
      return partialReceiver.onMessage(source, path, t, flags, message);
    }
  }

  @Override
  public boolean passMessageDownstream(Object object, MPIMessage currentMessage) {
    return true;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, pathToUse, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean send(int source, Object message, int flags, int dest) {
    return delegete.sendMessage(source, message, dest, flags,
        sendRoutingParameters(source, dest));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int dest) {
    return delegete.sendMessagePartial(source, message, dest, flags,
        partialSendRoutingParameters(source, dest));
  }


  /**
   * Initialize
   * @param cfg
   * @param t
   * @param taskPlan
   * @param edge
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this.type = t;
    this.instancePlan = taskPlan;
    this.executor = taskPlan.getThisExecutor();
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

    Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, MPIMessage>>> pendingReceiveMessagesPerSource = new HashMap<>();
    Map<Integer, Queue<MPIMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new MPIMultiMessageSerializer(new KryoSerializer(), executor));
    }

    int maxReceiveBuffers = MPIContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }

    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<Pair<Object, MPIMessage>>(capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<MPIMessage>(capacity));
      deSerializerMap.put(e, new MPIMultiMessageDeserializer(new KryoSerializer(), executor));
    }

    Set<Integer> sourcesOfThisExec = TaskPlanUtils.getTasksOfThisExecutor(taskPlan, sources);
    for (int s : sourcesOfThisExec) {
      sendRoutingParameters(s, pathToUse);
      partialSendRoutingParameters(s, pathToUse);
    }

    delegete.init(cfg, t, taskPlan, edge,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
    delegete.setKeyType(keyType);
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
    try {
      delegete.progress();

      if (lock.tryLock()) {
        try {
          finalReceiver.progress();
        } finally {
          lock.unlock();
        }
      }

      if (partialLock.tryLock()) {
        try {
          partialReceiver.progress();
        } finally {
          partialLock.unlock();
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void finish() {

  }

  @Override
  public MessageType getType() {
    return type;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return instancePlan;
  }

  @Override
  public void setMemoryMapped(boolean memoryMapped) {
    delegete.setStoreBased(memoryMapped);
  }
}
