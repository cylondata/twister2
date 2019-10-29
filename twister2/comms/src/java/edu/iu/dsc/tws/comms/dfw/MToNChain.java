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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.Deserializers;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.Serializers;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class MToNChain implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(MToNChain.class.getName());

  /**
   * Locally merged results
   */
  private Int2ObjectArrayMap<Queue<AggregatedObjects<Object>>> merged = new Int2ObjectArrayMap<>();

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
  private Map<Integer, IntArrayList> workerToTargets = new HashMap<>();

  /**
   * Worker to sources
   */
  private Map<Integer, IntArrayList> workerToSources = new HashMap<>();

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
   * The receive groups
   */
  private List<IntArrayList> receiveGroupsWorkers = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<IntArrayList> sendingGroupsWorkers = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<IntArrayList> sendingGroupsTargets = new ArrayList<>();

  /**
   * The sending groups
   */
  private List<IntArrayList> receiveGroupsSources = new ArrayList<>();

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
  private IntArraySet syncedSources = new IntArraySet();

  /**
   * The sends completed for this step
   */
  private int competedSends;

  /**
   * Number of sends needs to complete
   */
  private IntList sendsNeedsToComplete = new IntArrayList();

  /**
   * The receives completed for this step
   */
  private int competedReceives;

  /**
   * The receives needs to be completed for this round
   */
  private IntList receivesNeedsToComplete = new IntArrayList();

  /**
   * Number of sources per worker
   */
  private Int2IntArrayMap sourcesPerWorker = new Int2IntArrayMap();

  /**
   * Source array for iterations
   */
  private int[] thisSourceArray;

  /**
   * Keep track what are the targets we've sent syncs to
   */
  private IntOpenHashSet syncSent = new IntOpenHashSet();

  /**
   * Keep state
   */
  private Int2ObjectArrayMap<ReceiverState> sourceStates = new Int2ObjectArrayMap<>();

  /**
   * The targets
   */
  private int[] targetsArray;

  /**
   * Number of messages added in this step
   */
  private int mergerInMemoryMessages;

  /**
   * Number of messages in the merged memory
   */
  private int mergedInMemoryMessages;

  /**
   * When this configuration limit is reached, we will call progress on merger
   */
  private int inMemoryMessageThreshold;

  /**
   * keep track of finished sources
   */
  private IntArraySet mergeFinishSources = new IntArraySet();

  /**
   * Weather all syncs received from the sources
   */
  private boolean thisSourcesSynced = false;

  /**
   * The configuration
   */
  private Config config;

  /**
   * Weather merger returned false
   */
  private boolean mergerBlocked = false;


  /**
   * Keep track of the finished send groups
   */
  private IntArraySet finishedSendGroups = new IntArraySet();

  /**
   * Keep track of the finished receive groups
   */
  private IntArraySet finishedReceiveGroups = new IntArraySet();

  /**
   * Started the sync round
   */
  private boolean startedSyncRound = false;

  /**
   * Weather we have finished receiving, all syncs are available
   */
  private boolean finishedReceiving = false;

  /**
   * We are done with progress until we reset
   */
  private boolean doneProgress = false;

  /**
   * Finished workers per target (target -> finished workers)
   */
  private Int2ObjectOpenHashMap<Set<Integer>> finishedSources = new Int2ObjectOpenHashMap<>();

  /**
   * After all the sources finished for a target we add to this set
   */
  private IntArrayList finishedTargets = new IntArrayList();

  /**
   * Keep track weather we have started receive syncs to the final destination
   */
  private boolean receivingFinalSyncs = false;

  /**
   * Keep track weather all targets received syncs. After we start receiving syncs
   * we can only progress final receiver after all targets received syncs and we are
   * done with the rounds.
   */
  private boolean allTargetsReceivedSyncs = false;

  /**
   * Targets of this worker
   */
  private IntArraySet targetsOfThisWorker;

  /**
   * Low water mark
   */
  private int lowWaterMark;

  /**
   * High water mark to keep track of objects
   */
  private int highWaterMark;

  /**
   * The grouping size
   */
  private long groupingSize;

  /**
   * Message schema to be used
   */
  private MessageSchema messageSchema;

  /**
   * The channel
   */
  private TWSChannel channel;

  private int roundNumber = 0;

  /**
   * Weather we are in the last round
   */
  private boolean lastRound = false;

  private boolean roundCompleted = false;

  /**
   * Progress states
   */
  private enum ProgressState {
    MERGED,
    ROUND_DONE,
    SYNC_STARTED
  }

  /**
   * Progress state
   */
  private ProgressState progressState = ProgressState.ROUND_DONE;

  /**
   * Empty data set to send
   */
  private AggregatedObjects<Object> empty = new AggregatedObjects<>();

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
  public MToNChain(Config cfg, TWSChannel channel, LogicalPlan tPlan, Set<Integer> sources,
                   Set<Integer> targets, MessageReceiver finalRcvr,
                   MessageReceiver partialRcvr,
                   MessageType dType, MessageType rcvType,
                   MessageType kType, MessageType rcvKType, int edge,
                   MessageSchema messageSchema) {
    this.merger = partialRcvr;
    this.finalReceiver = finalRcvr;
    this.taskPlan = tPlan;
    this.dataType = dType;
    this.keyType = kType;
    this.sources = sources;
    this.targets = targets;
    this.receiveDataType = rcvType;
    this.receiveKeyType = rcvKType;
    this.config = cfg;
    this.channel = channel;
    this.inMemoryMessageThreshold =
        CommunicationContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    lowWaterMark = CommunicationContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = CommunicationContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);
    this.groupingSize = CommunicationContext.getNetworkPartitionBatchGroupingSize(cfg);
    this.messageSchema = messageSchema;
    if (highWaterMark - lowWaterMark <= groupingSize) {
      groupingSize = highWaterMark - lowWaterMark - 1;
      LOG.fine("Changing the grouping size to: " + groupingSize);
    }

    // this worker
    this.thisWorker = tPlan.getThisWorker();

    // get the tasks of this executor
    targetsOfThisWorker = new IntArraySet(TaskPlanUtils.getTasksOfThisWorker(tPlan, targets));
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
    int index = 0;
    thisSourceArray = new int[thisWorkerSources.size()];
    for (int s : thisWorkerSources) {
      this.thisSourceArray[index++] = s;
      sourceStates.put(s, ReceiverState.INIT);
    }

    this.targetsArray = new int[targets.size()];
    index = 0;
    for (int t : targets) {
      targetsArray[index++] = t;
    }

    // calculate the workers from we are receiving
    Set<Integer> receiveWorkers = TaskPlanUtils.getWorkersOfTasks(tPlan, sources);
    receiveWorkers.remove(taskPlan.getThisWorker());

    Map<Integer, ArrayBlockingQueue<OutMessage>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    for (int s : sourcesOfThisWorker) {
      // later look at how not to allocate pairs for this each time
      pendingSendMessagesPerSource.put(s, new ArrayBlockingQueue<>(
          CommunicationContext.sendPendingMax(cfg)));
      serializerMap.put(s, Serializers.get(isKeyed, this.messageSchema));
    }

    int maxReceiveBuffers = CommunicationContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receiveWorkers.size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    for (int ex : sources) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      pendingReceiveMessagesPerSource.put(ex, new ArrayBlockingQueue<>(capacity));
      pendingReceiveDeSerializations.put(ex, new ArrayBlockingQueue<>(capacity));
      deSerializerMap.put(ex, Deserializers.get(isKeyed, this.messageSchema));
    }

    // calculate the receive groups
    calculateReceiveGroups();

    for (int t : targets) {
      merged.put(t, new LinkedList<>());
      finishedSources.put(t, new HashSet<>());
    }

    if (thisWorker == 0) {
      LOG.info("Starting ring algorithm");
    }

    // create the delegate
    this.delegate = new ControlledChannelOperation(channel, cfg, dataType,
        rcvType, kType, rcvKType, tPlan, edge, receiveWorkers,
        this, pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed,
        sendingGroupsTargets, receiveGroupsSources, receiveGroupsWorkers);

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

    int group = CommunicationContext.getRingWorkersPerGroup(config);
    int numGroups = (int) Math.min(Math.ceil(sendingWorkers.size() / (group * 1.0)),
        Math.ceil(receiveWorkers.size() / (group * 1.0)));

    sendGroupIndex = createGroup(sendingWorkersSorted, numGroups, sendingGroupsWorkers);
    receiveGroupIndex = createGroup(receiveWorkersSorted, numGroups, receiveGroupsWorkers);

    int index = 0;
    for (List<Integer> sendGroup : sendingGroupsWorkers) {
      IntArrayList ts = new IntArrayList();
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
      IntArrayList srcs = new IntArrayList();
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

  private int createGroup(List<Integer> sendingWorkersSorted, int numGroups,
                          List<IntArrayList> groups) {
    int valuesPerGroup = sendingWorkersSorted.size() / numGroups;
    IntArrayList list = new IntArrayList();
    int thisGroup = 0;
    for (int i = 0; i < sendingWorkersSorted.size(); i++) {
      Integer wId = sendingWorkersSorted.get(i);
      list.add(wId);

      if (list.size() == 1) {
        groups.add(list);
      }

      if (wId == thisWorker) {
        thisGroup = groups.size() - 1;
      }

      if (list.size() == valuesPerGroup) {
        list = new IntArrayList();
      }
    }
    return thisGroup;
  }

  private void calculateWorkerIdToTargets(Set<Integer> logicalIds,
                                          Map<Integer, IntArrayList> workerToIds,
                                          Map<Integer, Integer> idToWorkers) {
    for (int t : logicalIds) {
      int worker = taskPlan.getWorkerForForLogicalId(t);
      IntArrayList ts;
      if (workerToIds.containsKey(worker)) {
        ts = workerToIds.get(worker);
      } else {
        ts = new IntArrayList();
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
    if (merger.onMessage(source, 0, target, flags, message)) {
      if ((flags & MessageFlags.SYNC_EMPTY) != MessageFlags.SYNC_EMPTY) {
        mergerInMemoryMessages++;
      }
      mergerBlocked = false;
      return true;
    } else {
      mergerBlocked = true;
      return false;
    }
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
      syncedSources.add(source);
      if (syncedSources.size() == thisSourceArray.length) {
        thisSourcesSynced = true;
      }
      return true;
    }

    if (mergedInMemoryMessages >= highWaterMark * targetsArray.length) {
      return false;
    }

    // we add to the merged
    Queue<AggregatedObjects<Object>> messages = merged.get(target);
    if (message instanceof AggregatedObjects) {
      messages.add((AggregatedObjects) message);
      mergedInMemoryMessages += ((AggregatedObjects) message).size();
      mergerInMemoryMessages -= ((AggregatedObjects) message).size();
    } else {
      throw new RuntimeException("Un-expected message");
    }
    return true;
  }

  private boolean sendSyncs() {
    boolean sent = false;
    IntArrayList sendingGroup = sendingGroupsWorkers.get(sendGroupIndex);
    for (int j = 0; j < sendingGroup.size(); j++) {
      int worker = sendingGroup.getInt(j);
      IntArrayList workerTargets = workerToTargets.get(worker);
      int targetIndex = sendWorkerTaskIndex.get(worker);

      for (int i = targetIndex; i < workerTargets.size(); i++) {
        int target = workerTargets.getInt(i);
        RoutingParameters parameters = targetRoutes.get(target);
        byte[] message = new byte[1];
        int flags = MessageFlags.SYNC_EMPTY;
        if (delegate.sendMessage(representSource, message, target, flags, parameters)) {
          sent = true;
          // advance the index
          targetIndex++;
          syncSent.add(target);
          sendWorkerTaskIndex.put(worker, targetIndex);
        } else {
          // no point in going further
          return false;
        }
      }
    }


    if (sent) {
      if (syncSent.size() == targetsArray.length) {
        for (int source : thisWorkerSources) {
          sourceStates.put(source, ReceiverState.SYNCED);
        }
      }
      finishedSendGroups.add(sendGroupIndex);
    }
    return true;
  }

  private int decrement(int groupIndex, int size) {
    return (groupIndex + size - 1) % size;
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
    // lets progress the controlled operation
    boolean completed = false;
    boolean needFurtherMerging = true;
    if (doneProgress) {
      return false;
    }
    // if we have enough things in memory or some sources finished lets call progress on merger
    int sendsToComplete = sendsNeedsToComplete.get(sendGroupIndex);
    // we can call merge only after a round is done
    if (progressState == ProgressState.ROUND_DONE
        && (mergerInMemoryMessages >= highWaterMark * targetsArray.length
        || mergerBlocked || mergeFinishSources.size() > 0)) {
      merger.progress();
      needFurtherMerging = !merger.isComplete();
      progressState = ProgressState.MERGED;
    }

    // now we can send to group
    boolean syncsDone;
    boolean sendsDone = true;
    if (progressState == ProgressState.MERGED) {
      sendsDone = sendToGroup();
    } else if (progressState == ProgressState.SYNC_STARTED) {
      syncsDone = sendSyncs();
      sendsDone = syncsDone;
      needFurtherMerging = false;
    }

    // progress the send
    boolean sendsCompleted = competedSends == sendsToComplete;
    boolean receiveCompleted =
        receivesNeedsToComplete.getInt(receiveGroupIndex) == competedReceives;

    int count = 0;
    while (count < 4 && !completed) {
      if (!sendsCompleted) {
        for (int i = 0; i < thisSourceArray.length; i++) {
          delegate.sendProgress(thisSourceArray[i]);
        }
        // we need to have a lock when accessing the channel as
        // it is not thread safe
        lock.lock();
        try {
          channel.progressSends();
        } finally {
          lock.unlock();
        }
      }

      if (!receiveCompleted) {
        IntArrayList receiveList = receiveGroupsSources.get(receiveGroupIndex);
        for (int i = 0; i < receiveList.size(); i++) {
          int receiveId = receiveList.getInt(i);
          delegate.receiveDeserializeProgress(receiveId);
          delegate.receiveProgress(receiveId);
        }
        lock.lock();
        try {
          channel.progressReceives(receiveGroupIndex);
        } finally {
          lock.unlock();
        }
      }

      sendsCompleted = competedSends == sendsToComplete;
      receiveCompleted = receivesNeedsToComplete.getInt(receiveGroupIndex) == competedReceives;

      // lets try to send the syncs
      if (sendsDone && sendsCompleted && receiveCompleted) {
        completed = true;
      }
      count++;
    }

    // lets progress the last receiver at last
    if (!receivingFinalSyncs) {
      finalReceiver.progress();
    }

    // if this step is completed and we need to progress the final receiver
    boolean needsProgress = !allTargetsReceivedSyncs || needFurtherMerging;
    if (completed && !needsProgress) {
      // LOG.info(thisWorker + " Finished receiving");
      finishedReceiving = true;
    }

    if (completed) {
      finishedReceiveGroups.add(receiveGroupIndex);

      // lets advance the send group and receive group
      if (finishedSendGroups.size() == sendingGroupsWorkers.size()
          && finishedReceiveGroups.size() == receiveGroupsWorkers.size()) {
        if (thisSourcesSynced && !containsAnyDataToSend()) {
          startedSyncRound = true;
          progressState = ProgressState.SYNC_STARTED;
        } else {
          progressState = ProgressState.ROUND_DONE;
        }
        roundCompleted = true;
        roundNumber++;
        // LOG.info(thisWorker + " Round of send and receive done " + roundNumber);
        finishedReceiveGroups.clear();
        finishedSendGroups.clear();

        // we need to have all the syncs received in a single round to terminate
        if (startedSyncRound && !allTargetsReceivedSyncs) {
          finishedTargets.clear();
          for (int i : targetsOfThisWorker) {
            Set<Integer> fin = finishedSources.get(i);
            fin.clear();
          }
        }
      }

      if (roundCompleted && !lastRound && finishedReceiving) {
        lastRound = true;
      } else if (lastRound && roundCompleted) {
        // at last we wait until everything is flushed
        if (delegate.isComplete()) {
          doneProgress = true;
        } else {
          delegate.progress();
          lock.lock();
          try {
            channel.progress();
          } finally {
            lock.unlock();
          }
        }
      }

      if (!lastRound || !roundCompleted) {
        sendGroupIndex = decrement(sendGroupIndex, sendingGroupsWorkers.size());
        receiveGroupIndex = increment(receiveGroupIndex, receiveGroupsWorkers.size());
        if (roundCompleted) {
          LOG.info("Starting round number: " + roundNumber);
        }
        roundCompleted = false;
        startNextStep();
      }
    }

    boolean progress = true;
    if (doneProgress) {
      finalReceiver.progress();
      progress = !finalReceiver.isComplete();
    }

    return progress;
  }

  private boolean containsAnyDataToSend() {
    for (int i = 0; i < targetsArray.length; i++) {
      int t = targetsArray[i];
      Queue<AggregatedObjects<Object>> data = merged.get(t);
      if (data != null && data.size() > 0) {
        return true;
      }
    }
    return false;
  }

  private boolean sendToGroup() {
    boolean sent = false;
    IntArrayList sendingGroup = sendingGroupsWorkers.get(sendGroupIndex);
    for (int j = 0; j < sendingGroup.size(); j++) {
      int worker = sendingGroup.getInt(j);
      IntArrayList workerTargets = workerToTargets.get(worker);
      int targetIndex = sendWorkerTaskIndex.get(worker);

      for (int i = targetIndex; i < workerTargets.size(); i++) {
        int target = workerTargets.get(i);
        Queue<AggregatedObjects<Object>> queue = merged.get(target);

        AggregatedObjects<Object> data = queue.poll();
        if (data == null) {
          data = empty;
        }
        RoutingParameters parameters = targetRoutes.get(target);
        // even if we have 0 tuples, we need to send at this point
        if (!delegate.sendMessage(representSource, data, target, 0, parameters)) {
          return false;
        } else {
          sent = true;
          // we are going to decrease the amount of messages in memory
          mergedInMemoryMessages -= data.size();
          // advance the index
          targetIndex++;
          sendWorkerTaskIndex.put(worker, targetIndex);
        }
      }
    }

    if (sent) {
      finishedSendGroups.add(sendGroupIndex);
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
    mergeFinishSources.clear();
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

    mergeFinishSources.clear();
    allTargetsReceivedSyncs = false;
    receivingFinalSyncs = false;
    doneProgress = false;
    finishedTargets.clear();
    startedSyncRound = false;

    for (int t : targets) {
      merged.put(t, new LinkedList<>());
      finishedSources.put(t, new HashSet<>());
    }

    finishedReceiveGroups.clear();
    finishedSendGroups.clear();
    mergerBlocked = false;
    thisSourcesSynced = false;
    mergeFinishSources.clear();
    finishedReceiving = false;

    int index = 0;
    syncSent.clear();
    for (int s : thisWorkerSources) {
      this.thisSourceArray[index++] = s;
      sourceStates.put(s, ReceiverState.INIT);
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
    if (!doneProgress) {
      return false;
    } else {
      return delegate.isComplete();
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
  public Set<Integer> getSources() {
    return sources;
  }

  @Override
  public Set<Integer> getTargets() {
    return targets;
  }

  @Override
  public boolean receiveMessage(MessageHeader header, Object object) {
    if (finishedReceiving) {
      competedReceives++;
      return true;
    }

    int flags = header.getFlags();
    if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
      int worker = sourcesToWorkers.get(header.getSourceId());
      IntArrayList sourcesOfWorker = workerToSources.get(worker);
      for (int src : sourcesOfWorker) {
        boolean recv = finalReceiver.onMessage(src,
            CommunicationContext.DEFAULT_DESTINATION,
            header.getDestinationIdentifier(), header.getFlags(), object);
        if (recv) {
          addSync(src, header.getDestinationIdentifier());
        } else {
          throw new RuntimeException("Sync should be accepted");
        }
      }
      competedReceives++;
      return true;
    } else if ((flags & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
      boolean recv = finalReceiver.onMessage(header.getSourceId(),
          CommunicationContext.DEFAULT_DESTINATION,
          header.getDestinationIdentifier(), header.getFlags(), object);
      if (recv) {
        competedReceives++;
      }
      return recv;
    }

    if (object instanceof AggregatedObjects) {
      if (((AggregatedObjects) object).size() > 0) {
        boolean recv = finalReceiver.onMessage(header.getSourceId(),
            CommunicationContext.DEFAULT_DESTINATION,
            header.getDestinationIdentifier(), header.getFlags(), object);
        if (recv) {
          competedReceives++;
        }
        return recv;
      } else {
        competedReceives++;
        return true;
      }
    } else if (object == null) {
      competedReceives++;
      return true;
    } else {
      throw new RuntimeException("we can only receive Aggregator objects");
    }
  }

  @Override
  public boolean receiveSendInternally(int source, int target, int path,
                                       int flags, Object message) {
    if (finishedReceiving) {
      competedReceives++;
      return true;
    }
    if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
      int worker = sourcesToWorkers.get(source);
      IntArrayList sourcesOfWorker = workerToSources.get(worker);
      for (int src : sourcesOfWorker) {
        boolean recv = finalReceiver.onMessage(src, 0, target, flags, message);
        if (recv) {
          addSync(src, target);
        } else {
          throw new RuntimeException("Sync should be accepted");
        }
      }
      competedReceives++;
      return true;
    }

    if (message instanceof AggregatedObjects) {
      if (((AggregatedObjects) message).size() > 0) {
        boolean recv = finalReceiver.onMessage(source, 0, target, flags, message);
        if (recv) {
          competedReceives++;
        }
        return recv;
      } else {
        // we received an empty value, lets increase but not call final receiver
        competedReceives++;
        return true;
      }
    } else if (message == null) {
      competedReceives++;
      return true;
    } else {
      throw new RuntimeException("we can only receive Aggregator objects");
    }
  }

  private void addSync(int source, int target) {
    receivingFinalSyncs = true;
    Set<Integer> finished = finishedSources.get(target);
    if (finished.contains(source)) {
      LOG.log(Level.FINE,
          String.format("%d Duplicate finish from source id %d -> %d",
              this.thisWorker, source, target));
    } else {
      finished.add(source);
    }
    if (finished.size() == sources.size()) {
      if (!finishedTargets.contains(target)) {
        finishedTargets.add(target);
      }

      if (finishedTargets.size() == targetsOfThisWorker.size()) {
        allTargetsReceivedSyncs = true;
      }
    }
  }

  @Override
  public boolean isDelegateComplete() {
    return delegate.isComplete();
  }

  @Override
  public void finish(int source) {
    // add to finished sources
    mergeFinishSources.add(source);

    Set<Integer> tOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(taskPlan, targets);
    for (int dest : tOfThisWorker) {
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
