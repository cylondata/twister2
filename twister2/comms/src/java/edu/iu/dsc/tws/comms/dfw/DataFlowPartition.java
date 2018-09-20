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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
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
import edu.iu.dsc.tws.comms.op.EdgeGenerator;
import edu.iu.dsc.tws.comms.op.OperationSemantics;
import edu.iu.dsc.tws.comms.routing.PartitionRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class DataFlowPartition implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(DataFlowPartition.class.getName());

  public enum PartitionStratergy {
    RANDOM,  // load balancing
    DIRECT,  // direct task based
  }

  /**
   * Partitioning stratergy
   */
  private PartitionStratergy partitionStratergy;

  /**
   * Sources
   */
  private Set<Integer> sources;

  /**
   * Destinations
   */
  private Set<Integer> destinations;

  /**
   * Partition router
   */
  private PartitionRouter router;

  /**
   * Destination index
   */
  private Map<Integer, Integer> destinationIndex;

  /**
   * Sources
   */
  private Set<Integer> thisSources;

  /**
   * The destinations
   */
  private Destinations dests = new Destinations();

  /**
   * Destinations
   */
  private List<Integer> destinationsList;

  /**
   * This tasks
   */
  private Set<Integer> thisTasks;

  /**
   * Final receiver
   */
  private MessageReceiver finalReceiver;

  /**
   * Partial receiver
   */
  private MessageReceiver partialReceiver;

  /**
   * The actual implementation
   */
  private ChannelDataFlowOperation delegete;

  /**
   * Configuration
   */
  private Config config;

  /**
   * Task plan
   */
  private TaskPlan instancePlan;

  /**
   * Executor ID
   */
  private int executor;

  /**
   * Receive message type, we can receive messages as just bytes
   */
  private MessageType receiveType;

  /**
   * Receive key type, we can receive keys as just bytes
   */
  private MessageType receiveKeyType;

  /**
   * Data type
   */
  private MessageType dataType;
  /**
   * Key type
   */
  private MessageType keyType;

  /**
   * Weather this is a key based communication
   */
  private boolean isKeyed;

  /**
   * Routing parameters are cached
   */
  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();

  /**
   * Routing parameters are cached
   */
  private Table<Integer, Integer, RoutingParameters> partialRoutingParamCache
      = HashBasedTable.create();

  /**
   * Lock for progressing the communication
   */
  private Lock lock = new ReentrantLock();

  /**
   * Lock for progressing the partial receiver
   */
  private Lock partialLock = new ReentrantLock();

  /**
   * Edge used for communication
   */
  private int edge;

  /**
   * The all reduce operation for sycnronizing at the end
   */
  private DataFlowAllReduce allReduce;

  /**
   * The operation semantics
   */
  private OperationSemantics opSemantics;

  /**
   * A place holder for keeping the internal and external destinations
   */
  @SuppressWarnings("VisibilityModifier")
  private class Destinations {
    List<Integer> internal = new ArrayList<>();
    List<Integer> external = new ArrayList<>();
  }

  public DataFlowPartition(TWSChannel channel, Set<Integer> sourceTasks, Set<Integer> destTasks,
                           MessageReceiver finalRcvr, MessageReceiver partialRcvr,
                           PartitionStratergy partitionStratergy,
                           MessageType dataType, MessageType keyType) {
    this(channel, sourceTasks, destTasks, finalRcvr, partialRcvr, partitionStratergy);
    this.isKeyed = true;
    this.keyType = keyType;
    this.dataType = dataType;
    this.receiveKeyType = keyType;
    this.receiveType = dataType;
  }

  public DataFlowPartition(TWSChannel channel, Set<Integer> sourceTasks, Set<Integer> destTasks,
                           MessageReceiver finalRcvr, MessageReceiver partialRcvr,
                           PartitionStratergy partitionStratergy,
                           MessageType dataType) {
    this(channel, sourceTasks, destTasks, finalRcvr, partialRcvr, partitionStratergy);
    this.dataType = dataType;
  }

  public DataFlowPartition(TWSChannel channel, Set<Integer> srcs,
                           Set<Integer> dests, MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr,
                           PartitionStratergy stratergy) {
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);
    this.delegete = new ChannelDataFlowOperation(channel);
    this.partitionStratergy = stratergy;

    for (int s : sources) {
      destinationIndex.put(s, 0);
    }

    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
  }

  public DataFlowPartition(Config cfg, TWSChannel channel, TaskPlan tPlan, Set<Integer> srcs,
                           Set<Integer> dests, MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr,
                           PartitionStratergy strategy,
                           MessageType dType, MessageType rcvType,
                           OperationSemantics sem,
                           EdgeGenerator eGenerator) {
    this(cfg, channel, tPlan, srcs, dests, finalRcvr, partialRcvr, strategy, dType, rcvType,
        null, null, sem, eGenerator);
    this.isKeyed = false;
  }

  public DataFlowPartition(Config cfg, TWSChannel channel, TaskPlan tPlan, Set<Integer> srcs,
                           Set<Integer> dests, MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr,
                           PartitionStratergy strategy,
                           MessageType dType, MessageType rcvType,
                           MessageType kType, MessageType rcvKType,
                           OperationSemantics sem,
                           EdgeGenerator eGenerator) {
    this.instancePlan = tPlan;
    this.config = cfg;
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);
    this.delegete = new ChannelDataFlowOperation(channel);
    this.partitionStratergy = strategy;
    this.dataType = dType;
    this.receiveType = rcvType;
    this.keyType = kType;
    this.receiveKeyType = rcvKType;
    this.edge = eGenerator.nextEdge();
    this.opSemantics = sem;

    if (keyType != null) {
      this.isKeyed = true;
    }

    // sources
    for (int src : sources) {
      destinationIndex.put(src, 0);
    }

    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;

    init(cfg, dType, instancePlan, edge);
  }

  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int ed) {
    this.edge = ed;
    this.thisSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    LOG.log(Level.FINE, String.format("%d setup loadbalance routing %s %s",
        taskPlan.getThisExecutor(), sources, destinations));
    this.thisTasks = taskPlan.getTasksOfThisExecutor();
    this.router = new PartitionRouter(taskPlan, sources, destinations);
    Map<Integer, Set<Integer>> internal = router.getInternalSendTasks(0);
    Map<Integer, Set<Integer>> external = router.getExternalSendTasks(0);
    this.instancePlan = taskPlan;
    this.dataType = t;
    if (this.receiveType == null) {
      this.receiveType = dataType;
    }

    LOG.log(Level.FINE, String.format("%d adding internal/external routing",
        taskPlan.getThisExecutor()));
    for (int s : thisSources) {
      Set<Integer> integerSetMap = internal.get(s);
      if (integerSetMap != null) {
        this.dests.internal.addAll(integerSetMap);
      }

      Set<Integer> integerSetMap1 = external.get(s);
      if (integerSetMap1 != null) {
        this.dests.external.addAll(integerSetMap1);
      }
      LOG.fine(String.format("%d adding internal/external routing %d",
          taskPlan.getThisExecutor(), s));
      break;
    }

    LOG.log(Level.FINE, String.format("%d done adding internal/external routing",
        taskPlan.getThisExecutor()));
    if (this.finalReceiver != null && isLastReceiver()) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    }
    if (this.partialReceiver != null) {
      this.partialReceiver.init(cfg, this, receiveExpectedTaskIds());
    }


    Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, ChannelMessage>>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<ChannelMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    Set<Integer> tempsrcs = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);

    //need to set minus tasks as well
    for (Integer src : tempsrcs) {
      srcs.add((src * -1) - 1);
    }
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
    for (int ex : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<Pair<Object, ChannelMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<Pair<Object, ChannelMessage>>(
              capacity);
      pendingReceiveMessagesPerSource.put(ex, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(ex, new ArrayBlockingQueue<ChannelMessage>(capacity));
      deSerializerMap.put(ex, new MultiMessageDeserializer(new KryoSerializer(), executor));
    }

    for (int src : srcs) {
      for (int dest : destinations) {
        sendRoutingParameters(src, dest);
      }
    }

    delegete.init(cfg, t, receiveType, keyType, receiveKeyType, taskPlan, edge,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
    delegete.setKeyType(keyType);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    int newFlags = flags | MessageFlags.ORIGIN_PARTIAL;
    return delegete.sendMessagePartial(source, message, 0,
        newFlags, sendPartialRoutingParameters(source, 0));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    int newFlags = flags | MessageFlags.ORIGIN_PARTIAL;
    return delegete.sendMessagePartial(source, message, target, newFlags,
        sendPartialRoutingParameters(source, target));
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    int newFlags = flags | MessageFlags.ORIGIN_SENDER;
    return delegete.sendMessage(source, message, 0, newFlags, sendRoutingParameters(source, 0));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    int newFlags = flags | MessageFlags.ORIGIN_SENDER;
    return delegete.sendMessage(source, message, target, newFlags,
        sendRoutingParameters(source, target));
  }

  public boolean isComplete() {
    boolean done = delegete.isComplete();
    boolean needsFurtherProgress = OperationUtils.progressReceivers(delegete, lock, finalReceiver,
        partialLock, partialReceiver);
    return done && !needsFurtherProgress;
  }

  public boolean isDelegeteComplete() {
    return delegete.isComplete();
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
    // first we need to call finish on the partial receivers
    if (partialReceiver != null) {
      partialReceiver.onFinish(source);
    }
  }

  @Override
  public TaskPlan getTaskPlan() {
    return instancePlan;
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
    if (routingParamCache.contains(source, path)) {
      return routingParamCache.get(source, path);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();
      if (partitionStratergy == PartitionStratergy.RANDOM) {
        routingParameters.setDestinationId(0);
        if (!destinationIndex.containsKey(source)) {
          throw new RuntimeException(String.format(
              "Un-expected source %d in loadbalance executor %d %s", source,
              executor, destinationIndex));
        }

        int index = destinationIndex.get(source);
        int route = destinationsList.get(index);

        if (thisTasks.contains(route)) {
          routingParameters.addInteranlRoute(route);
        }

        routingParameters.setDestinationId(route);

        index = (index + 1) % destinations.size();
        destinationIndex.put(source, index);
      } else if (partitionStratergy == PartitionStratergy.DIRECT) {
        routingParameters.setDestinationId(path);
        routingParameters.addInteranlRoute(source);

      }
      routingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  private RoutingParameters sendPartialRoutingParameters(int source, int destination) {
    if (partialRoutingParamCache.contains(source, destination)) {
      return partialRoutingParamCache.get(source, destination);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();
      if (partitionStratergy == PartitionStratergy.RANDOM) {
        routingParameters.setDestinationId(0);
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

        index = (index + 1) % destinations.size();
        destinationIndex.put(source, index);
      } else if (partitionStratergy == PartitionStratergy.DIRECT) {
        routingParameters.setDestinationId(destination);
        if (dests.external.contains(destination)) {
          routingParameters.addExternalRoute(destination);
        } else {
          routingParameters.addInteranlRoute(destination);
        }
      }
      partialRoutingParamCache.put(source, destination, routingParameters);
      return routingParameters;
    }
  }

  /**
   * For partial receives the path and
   * @param source
   * @param path
   * @param destination
   * @param flags
   * @param message
   * @return
   */
  public boolean receiveSendInternally(int source, int path,
                                       int destination, int flags, Object message) {
    // okay this must be for the
    if ((flags & MessageFlags.ORIGIN_PARTIAL) == MessageFlags.ORIGIN_PARTIAL) {
      return finalReceiver.onMessage(source, path, destination, flags, message);
    }
    return partialReceiver.onMessage(source, path, destination, flags, message);
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
        header.getDestinationIdentifier(), header.getFlags(), object);
  }

  protected boolean isLastReceiver() {
    return true;
  }

  public Set<Integer> getSources() {
    return sources;
  }

  public Set<Integer> getDestinations() {
    return destinations;
  }

  @Override
  public MessageType getKeyType() {
    return keyType;
  }

  @Override
  public MessageType getDataType() {
    return dataType;
  }


  public int getEdge() {
    return edge;
  }
}
