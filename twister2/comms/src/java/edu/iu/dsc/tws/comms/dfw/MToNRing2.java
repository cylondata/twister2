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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.channel.ChannelReceiver;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageDeSerializer;
import edu.iu.dsc.tws.api.comms.packing.MessageSerializer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.DataSerializer;
import edu.iu.dsc.tws.comms.dfw.io.KeyedDataDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.KeyedDataSerializer;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MToNRing2 implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(MToNRing2.class.getName());

  /**
   * Locally merged results
   */
  private Map<Integer, List<Object>> merged = new HashMap<>();

  /**
   * This is the local merger
   */
  private MessageReceiver merger;

  /**
   * Final receiver
   */
  private MessageReceiver finalReceiver;

  /**
   * The actual implementation
   */
  private ControlledChannelOperation delegate;

  /**
   * Lock for progressing the communication
   */
  private Lock lock = new ReentrantLock();

  /**
   * Lock for progressing the partial receiver
   */
  private Lock partialLock = new ReentrantLock();

  /**
   * The task plan
   */
  private LogicalPlan taskPlan;

  /**
   * A map holding workerId to targets
   */
  private Map<Integer, List<Integer>> workerToTargets = new HashMap<>();

  /**
   * Worker to sources
   */
  private Map<Integer, List<Integer>> workerToSources = new HashMap<>();

  /**
   * Targets to workers
   */
  private Map<Integer, Integer> targetsToWorkers = new HashMap<>();

  /**
   * Sources to workers
   */
  private Map<Integer, Integer> sourcesToWorkers = new HashMap<>();

  /**
   * The target routes
   */
  private Map<Integer, RoutingParameters> targetRoutes = new HashMap<>();

  /**
   * The workers for targets, sorted
   */
  private List<Integer> targetWorkers;

  /**
   * This worker id
   */
  private int thisWorker;

  /**
   * The target index to send for each worker
   */
  private Map<Integer, Integer> sendWorkerTaskIndex = new HashMap<>();

  /**
   * The data type
   */
  private MessageType dataType;

  /**
   * The key type
   */
  private MessageType keyType;

  /**
   * The sources
   */
  private Set<Integer> sources;

  /**
   * The targets
   */
  private Set<Integer> targets;

  /**
   * The source representing this
   */
  private int representSource;

  /**
   * Weather keyed operation
   */
  private boolean isKeyed;

  /**
   * Lock the data strcutures while swapping
   */
  private Lock swapLock = new ReentrantLock();

  /**
   * Sources of this worker
   */
  private Set<Integer> thisWorkerSources;

  /**
   * Keep trck of receive data type for later return
   */
  private MessageType receiveDataType;

  /**
   * Keep track of receive key type for later return
   */
  private MessageType receiveKeyType;

  /**
   * The grouping
   */
  private int groupingSize;

  /**
   * The receive groups
   */
  private List<List<Integer>> receiveGroupsWorkers = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<List<Integer>> sendingGroupsWorkers = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<List<Integer>> sendingGroupsTargets = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<List<Integer>> receiveGroupsSources = new ArrayList<>();

  /**
   * Send group index
   */
  private int sendGroupIndex;

  /**
   * The receive group index
   */
  private int receiveGroupIndex;

  /**
   * Keep track of the sources that send the sync
   */
  private Set<Integer> syncedSources = new HashSet<>();

  /**
   * The sends completed for this step
   */
  private int competedSends;

  /**
   * Number of sends needs to complete
   */
  private List<Integer> sendsNeedsToComplete = new ArrayList<>();

  /**
   * The receives completed for this step
   */
  private int competedReceives;

  /**
   * The receives needs to be completed for this round
   */
  private List<Integer> receivesNeedsToComplete = new ArrayList<>();

  /**
   * Number of sources per worker
   */
  private Map<Integer, Integer> sourcesPerWorker = new HashMap<>();

  /**
   * Create a ring partition communication
   *
   * @param cfg configuration
   * @param channel channel
   * @param tPlan task plan
   * @param sources sources
   * @param targets targets
   * @param finalRcvr final receiver
   * @param partialRcvr partial receiver
   * @param dType data type
   * @param rcvType receive data type
   * @param kType key data type
   * @param rcvKType receive key type
   * @param edge the edge
   */
  public MToNRing2(Config cfg, TWSChannel channel, LogicalPlan tPlan, Set<Integer> sources,
                   Set<Integer> targets, MessageReceiver finalRcvr,
                   MessageReceiver partialRcvr,
                   MessageType dType, MessageType rcvType,
                   MessageType kType, MessageType rcvKType, int edge) {
    this.merger = partialRcvr;
    this.finalReceiver = finalRcvr;
    this.taskPlan = tPlan;
    this.dataType = dType;
    this.keyType = kType;
    this.sources = sources;
    this.targets = targets;
    this.receiveDataType = rcvType;
    this.receiveKeyType = rcvKType;
    this.groupingSize = DataFlowContext.getNetworkPartitionBatchGroupingSize(cfg);

    // this worker
    this.thisWorker = tPlan.getThisExecutor();

    // get the tasks of this executor
    Set<Integer> targetsOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(tPlan, targets);
    Set<Integer> sourcesOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(tPlan, sources);
    Map<Integer, List<Integer>> mergerExpectedIds = new HashMap<>();
    for (int target : targets) {
      mergerExpectedIds.put(target, new ArrayList<>(sourcesOfThisWorker));
    }
    // initialize the merger
    merger.init(cfg, this, mergerExpectedIds);

    // final receivers ids
    Map<Integer, List<Integer>> finalExpectedIds = new HashMap<>();
    for (int target : targetsOfThisWorker) {
      finalExpectedIds.put(target, new ArrayList<>(sources));
    }
    // initialize the final receiver
    finalReceiver.init(cfg, this, finalExpectedIds);

    // now calculate the worker id to target mapping
    calculateWorkerIdToTargets(targets, workerToTargets, targetsToWorkers);
    calculateWorkerIdToTargets(sources, workerToSources, sourcesToWorkers);

    // calculate the workers
    targetWorkers = new ArrayList<>(workerToTargets.keySet());
    Collections.sort(targetWorkers);

    // calculate the sources per worker
    List<Integer> sourceWorkers = new ArrayList<>(workerToSources.keySet());
    Collections.sort(sourceWorkers);
    for (int w : sourceWorkers) {
      if (w == thisWorker) {
        sourcesPerWorker.put(w, 0);
      } else {
        sourcesPerWorker.put(w, TaskPlanUtils.getTasksOfWorker(taskPlan, w, sources).size());
      }
    }

    // calculate the routes
    calculateRoutingParameters();

    // lets set the represent source here
    if (sourcesOfThisWorker.size() > 0) {
      representSource = sourcesOfThisWorker.iterator().next();
    }

    if (keyType != null) {
      isKeyed = true;
    }

    thisWorkerSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);

    // calculate the workers from we are receiving
    Set<Integer> receiveWorkers = TaskPlanUtils.getWorkersOfTasks(tPlan, sources);
    receiveWorkers.remove(taskPlan.getThisExecutor());

    Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    for (int s : sources) {
      // later look at how not to allocate pairs for this each time
      pendingSendMessagesPerSource.put(s, new ArrayBlockingQueue<>(
          DataFlowContext.sendPendingMax(cfg)));
      if (isKeyed) {
        serializerMap.put(s, new KeyedDataSerializer());
      } else {
        serializerMap.put(s, new DataSerializer());
      }
    }

    int maxReceiveBuffers = DataFlowContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receiveWorkers.size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    for (int ex : sources) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      pendingReceiveMessagesPerSource.put(ex, new ArrayBlockingQueue<>(capacity));
      pendingReceiveDeSerializations.put(ex, new ArrayBlockingQueue<>(capacity));
      if (isKeyed) {
        deSerializerMap.put(ex, new KeyedDataDeSerializer());
      } else {
        deSerializerMap.put(ex, new DataDeserializer());
      }
    }

    // calculate the receive groups
    calculateReceiveGroups();

    // create the delegate
    this.delegate = new ControlledChannelOperation(channel, cfg, dataType,
        rcvType, kType, rcvKType, tPlan, edge, receiveWorkers,
        this, pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed,
        sendingGroupsTargets, receiveGroupsSources);

    // start the first step
    startNextStep();
  }

  private void startNextStep() {
    List<Integer> sendWorkers = sendingGroupsWorkers.get(sendGroupIndex);
    // lets set the task indexes to 0
    for (int i : sendWorkers) {
      sendWorkerTaskIndex.put(i, 0);
    }

    // reset the completed sends and receives
    competedSends = 0;
    competedReceives = 0;

    // now configure the controlled operation to behave
    delegate.startGroup(receiveGroupIndex, sendGroupIndex, sourcesPerWorker);
  }

  private void calculateReceiveGroups() {
    Set<Integer> receiveWorkers = TaskPlanUtils.getWorkersOfTasks(taskPlan, sources);
    Set<Integer> sendingWorkers = TaskPlanUtils.getWorkersOfTasks(taskPlan, targets);

    List<Integer> receiveWorkersSorted = new ArrayList<>(receiveWorkers);
    List<Integer> sendingWorkersSorted = new ArrayList<>(sendingWorkers);

    receiveWorkersSorted.sort(new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1 - o2;
      }
    });

    sendingWorkersSorted.sort(new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o1 - o2;
      }
    });

    int group = 4;
    int numGroups = (int) Math.min(Math.ceil(sendingWorkers.size() / (group * 1.0)),
        Math.ceil(receiveWorkers.size() / (group * 1.0)));

    createGroup(sendingWorkersSorted, numGroups, sendingGroupsWorkers);
    createGroup(receiveWorkersSorted, numGroups, receiveGroupsWorkers);

    for (List<Integer> sendGroup : sendingGroupsWorkers) {
      List<Integer> ts = new ArrayList<>();
      // calculate the sends to complete
      int sendComplete = 0;
      for (int worker : sendGroup) {
        List<Integer> workerTargets = workerToTargets.get(worker);
        sendComplete += workerTargets.size();
        ts.addAll(workerTargets);
      }
      sendsNeedsToComplete.add(sendComplete);
      sendingGroupsTargets.add(ts);
    }

    for (List<Integer> receiveGroup : receiveGroupsWorkers) {
      List<Integer> srcs = new ArrayList<>();
      int receiveComplete = 0;
      // calculate the receives to complete
      for (int worker : receiveGroup) {
        List<Integer> workerSources = workerToSources.get(worker);
        receiveComplete += workerSources.size();
        srcs.addAll(workerSources);
      }
      receivesNeedsToComplete.add(receiveComplete);
      receiveGroupsSources.add(srcs);
    }
  }

  private void createGroup(List<Integer> sendingWorkersSorted, int numGroups,
                           List<List<Integer>> groups) {
    int valuesPerGroup = sendingWorkersSorted.size() / numGroups;
    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < sendingWorkersSorted.size(); i++) {
      if (list.size() == valuesPerGroup) {
        list = new ArrayList<>();
        groups.add(list);
      }
      list.add(sendingWorkersSorted.get(i));
    }
    groups.add(list);
  }

  private void calculateWorkerIdToTargets(Set<Integer> logicalIds,
                                          Map<Integer, List<Integer>> workerToIds,
                                          Map<Integer, Integer> idToWorkers) {
    for (int t : logicalIds) {
      int worker = taskPlan.getExecutorForChannel(t);
      List<Integer> ts;
      if (workerToIds.containsKey(worker)) {
        ts = workerToIds.get(worker);
      } else {
        ts = new ArrayList<>();
      }
      ts.add(t);
      workerToIds.put(worker, ts);
      idToWorkers.put(t, worker);
    }
  }

  private void calculateRoutingParameters() {
    this.targetRoutes = new HashMap<>();
    for (int t : targets) {
      RoutingParameters routingParameters = new RoutingParameters();

      Integer worker = targetsToWorkers.get(t);
      if (worker != thisWorker) {
        routingParameters.addExternalRoute(t);
      } else {
        routingParameters.addInteranlRoute(t);
      }
      routingParameters.setDestinationId(t);

      targetRoutes.put(t, routingParameters);
    }
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new UnsupportedOperationException("Operation is not supported");
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    throw new UnsupportedOperationException("Operation is not supported");
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    partialLock.lock();
    try {
      return merger.onMessage(source, 0, target, flags, message);
    } finally {
      partialLock.unlock();
    }
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    swapLock.lock();
    try {
      if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
        syncedSources.add(source);
        return true;
      }

      // we add to the merged
      List<Object> messages = merged.computeIfAbsent(target, k -> new AggregatedObjects<>());

      if (messages.size() < groupingSize) {
        if (message instanceof AggregatedObjects) {
          messages.addAll((Collection<?>) message);
        } else {
          messages.add(message);
        }
      } else {
        return false;
      }
    } finally {
      swapLock.unlock();
    }
    return true;
  }

  private int increment(int groupIndex, int size) {
    return (groupIndex + 1) % size;
  }

  @Override
  public void sendCompleted(Object message) {
    competedSends++;
  }

  @Override
  public boolean progress() {
    boolean completed = false;
    swapLock.lock();
    try {
      boolean sendDone = sendToGroup();
      boolean sendsCompleted = competedSends == sendsNeedsToComplete.get(sendGroupIndex);
      boolean receiveCompleted = receivesNeedsToComplete.get(receiveGroupIndex) == competedReceives;
      if (sendDone && sendsCompleted && receiveCompleted) {
        completed = true;
      }
      // if this step is completed start the next step
      if (completed) {
        sendGroupIndex = increment(sendGroupIndex, sendingGroupsWorkers.size());
        receiveGroupIndex = increment(receiveGroupIndex, receiveGroupsWorkers.size());
        // lets advance the send group and receive group
        startNextStep();
      }
    } finally {
      swapLock.unlock();
    }
    // now set the things
    return OperationUtils.progressReceivers(delegate, lock,
        finalReceiver, partialLock, merger) && !completed;
  }

  private boolean sendToGroup() {
    List<Integer> sendingGroup = sendingGroupsWorkers.get(sendGroupIndex);
    for (int worker : sendingGroup) {
      List<Integer> workerTargets = workerToTargets.get(worker);
      int targetIndex = sendWorkerTaskIndex.get(worker);

      for (int i = targetIndex; i < workerTargets.size(); i++) {
        int target = workerTargets.get(i);
        List<Object> data = merged.get(target);

        // just continue if null
        if (data == null) {
          continue;
        }

        if (data.size() == 0) {
          continue;
        }

        RoutingParameters parameters = targetRoutes.get(target);
        // even if we have 0 tuples, we need to send at this point
        if (!delegate.sendMessage(representSource, data, target, 0, parameters)) {
          return false;
        } else {
          merged.put(target, new AggregatedObjects<>());
          // advance the index
          targetIndex++;
          sendWorkerTaskIndex.put(worker, targetIndex);
        }
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (merged != null) {
      merger.close();
    }

    if (finalReceiver != null) {
      finalReceiver.close();
    }

    delegate.close();
  }

  @Override
  public void reset() {
    if (merged != null) {
      merger.clean();
    }

    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return taskPlan;
  }

  @Override
  public String getUniqueId() {
    return null;
  }

  @Override
  public boolean isComplete() {
    boolean done = delegate.isComplete();
    boolean needsFurtherProgress = OperationUtils.progressReceivers(delegate, lock, finalReceiver,
        partialLock, merger);
    return done && !needsFurtherProgress;
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
  public Set<Integer> getSources() {
    return sources;
  }

  @Override
  public Set<Integer> getTargets() {
    return targets;
  }

  @Override
  public boolean receiveMessage(MessageHeader header, Object object) {
    boolean recv = finalReceiver.onMessage(header.getSourceId(),
        DataFlowContext.DEFAULT_DESTINATION,
        header.getDestinationIdentifier(), header.getFlags(), object);
    if (recv) {
      competedReceives++;
    }
    return recv;
  }

  @Override
  public boolean receiveSendInternally(int source, int target, int path,
                                       int flags, Object message) {
    lock.lock();
    try {
      return finalReceiver.onMessage(source, 0, target, flags, message);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isDelegateComplete() {
    return delegate.isComplete();
  }

  @Override
  public void finish(int source) {
    Set<Integer> targetsOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(taskPlan, targets);
    for (int dest : targetsOfThisWorker) {
      // first we need to call finish on the partial receivers
      while (!send(source, new int[0], MessageFlags.SYNC_EMPTY, dest)) {
        // lets progress until finish
        progress();
      }
    }
  }

  @Override
  public MessageType getReceiveKeyType() {
    return receiveKeyType;
  }

  @Override
  public MessageType getReceiveDataType() {
    return receiveDataType;
  }
}
