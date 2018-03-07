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
import edu.iu.dsc.tws.comms.api.CompletionListener;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageSerializer;
import edu.iu.dsc.tws.comms.routing.PartitionRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MPIDataFlowPartition implements DataFlowOperation, MPIMessageReceiver {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowPartition.class.getName());

  public enum PartitionStratergy {
    RANDOM,  // load balancing
    DIRECT,  // direct task based
  }

  private PartitionStratergy partitionStratergy;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private PartitionRouter router;

  private Map<Integer, Integer> destinationIndex;
  private Set<Integer> thisSources;
  private Destinations dests = new Destinations();
  private List<Integer> destinationsList;
  private Set<Integer> thisTasks;
  private MessageReceiver finalReceiver;

  private MPIDataFlowOperation delegete;
  private Config config;
  private TaskPlan instancePlan;
  private int executor;
  private MessageType type;
  private MessageType keyType;
  private boolean isKeyed;
  private CompletionListener completionListener;
  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();
  private Lock lock = new ReentrantLock();

  /**
   * A place holder for keeping the internal and external destinations
   */
  @SuppressWarnings("VisibilityModifier")
  private class Destinations {
    List<Integer> internal = new ArrayList<>();
    List<Integer> external = new ArrayList<>();
  }

  public MPIDataFlowPartition(TWSChannel channel, Set<Integer> sourceTasks, Set<Integer> destTasks,
                              MessageReceiver receiver, PartitionStratergy partitionStratergy,
                              MessageType type, MessageType keyType) {
    this(channel, sourceTasks, destTasks, receiver, partitionStratergy);
    this.isKeyed = true;
    this.keyType = keyType;
    this.type = type;
  }

  public MPIDataFlowPartition(TWSChannel channel, Set<Integer> srcs,
                              Set<Integer> dests, MessageReceiver finalRcvr,
                              PartitionStratergy stratergy) {
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);
    this.delegete = new MPIDataFlowOperation(channel);
    this.partitionStratergy = stratergy;

    for (int s : sources) {
      destinationIndex.put(s, 0);
    }

    this.finalReceiver = finalRcvr;
  }

  public MPIDataFlowPartition(TWSChannel channel, Set<Integer> srcs,
                              Set<Integer> dests, MessageReceiver finalRcvr,
                              PartitionStratergy stratergy,
                              CompletionListener cmpListener) {
    this.sources = srcs;
    this.destinations = dests;
    this.destinationIndex = new HashMap<>();
    this.destinationsList = new ArrayList<>(destinations);
    this.delegete = new MPIDataFlowOperation(channel);
    this.partitionStratergy = stratergy;
    this.completionListener = cmpListener;
    for (int s : sources) {
      destinationIndex.put(s, 0);
    }

    this.finalReceiver = finalRcvr;
  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this.thisSources = TaskPlanUtils.getTasksOfThisExecutor(taskPlan, sources);
    LOG.info(String.format("%d setup loadbalance routing %s",
        taskPlan.getThisExecutor(), thisSources));
    this.thisTasks = taskPlan.getTasksOfThisExecutor();
    this.router = new PartitionRouter(taskPlan, sources, destinations);
    Map<Integer, Set<Integer>> internal = router.getInternalSendTasks(0);
    Map<Integer, Set<Integer>> external = router.getExternalSendTasks(0);
    this.instancePlan = taskPlan;
    this.type = t;

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
    //TODO : Does this send the correct receiveExpectedTaskIds for partition communication
    if (this.finalReceiver != null && isLastReceiver()) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, MPIMessage>>> pendingReceiveMessagesPerSource = new HashMap<>();
    Map<Integer, Queue<MPIMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisExecutor(taskPlan, sources);
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new MPIMessageSerializer(new KryoSerializer()));
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
          new ArrayBlockingQueue<Pair<Object, MPIMessage>>(
              capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<MPIMessage>(capacity));
      deSerializerMap.put(e, new MPIMessageDeSerializer(new KryoSerializer()));
    }

    for (int src : srcs) {
      for (int dest : destinations) {
        sendRoutingParameters(src, dest);
      }
    }

    delegete.setCompletionListener(completionListener);

    delegete.init(cfg, t, taskPlan, edge,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
    delegete.setKeyType(keyType);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int dest) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, 0, flags, sendRoutingParameters(source, 0));
  }

  @Override
  public boolean send(int source, Object message, int flags, int dest) {
    return delegete.sendMessage(source, message, dest, flags, sendRoutingParameters(source, dest));
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
        } else {
          routingParameters.addExternalRoute(route);
        }
        routingParameters.setDestinationId(route);

        index = (index + 1) % destinations.size();
        destinationIndex.put(source, index);
      } else if (partitionStratergy == PartitionStratergy.DIRECT) {
        routingParameters.setDestinationId(path);
        if (dests.external.contains(path)) {
          routingParameters.addExternalRoute(path);
        } else {
          routingParameters.addInteranlRoute(path);
        }
      }
      routingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  public boolean receiveSendInternally(int source, int t, int path, int flags, Object message) {
    // okay this must be for the
    return finalReceiver.onMessage(source, path, t, flags, message);
  }

  @Override
  public boolean passMessageDownstream(Object object, MPIMessage currentMessage) {
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

  public boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();
    return finalReceiver.onMessage(header.getSourceId(), MPIContext.DEFAULT_PATH,
        header.getDestinationIdentifier(), header.getFlags(), object);
  }

  protected boolean isLastReceiver() {
    return true;
  }
}
