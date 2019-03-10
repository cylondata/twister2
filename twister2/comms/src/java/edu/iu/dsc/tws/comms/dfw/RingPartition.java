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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class RingPartition implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(RingPartition.class.getName());

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
  private ChannelDataFlowOperation delegate;

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
  private TaskPlan taskPlan;

  /**
   * A map holding workerId to targets
   */
  private Map<Integer, List<Integer>> workerToTargets = new HashMap<>();

  /**
   * Targets to workers
   */
  private Map<Integer, Integer> targetsToWorkers = new HashMap<>();

  /**
   * The workers for targets, sorted
   */
  private List<Integer> workers;

  /**
   * The target index to send
   */
  private int targetIndex;

  /**
   * The next worker to send the data
   */
  private int nextWorker;

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

  public RingPartition(Config cfg, TWSChannel channel, TaskPlan tPlan, Set<Integer> sources,
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

    // get the tasks of this executor
    Set<Integer> targetsOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(tPlan, targets);
    Set<Integer> sourcesOfThisWorker = TaskPlanUtils.getTasksOfThisWorker(tPlan, sources);
    Map<Integer, List<Integer>> mergerExpectedIds = new HashMap<>();
    for (int target : targetsOfThisWorker) {
      mergerExpectedIds.put(target, new ArrayList<>(sourcesOfThisWorker));
    }
    // initialize the merger
    merger.init(cfg, this, mergerExpectedIds);

    // final receivers ids
    Map<Integer, List<Integer>> finalExpectedIds = new HashMap<>();
    for (int target : targets) {
      finalExpectedIds.put(target, new ArrayList<>(sources));
    }
    // initialize the final receiver
    finalReceiver.init(cfg, this, finalExpectedIds);

    // now calculate the worker id to target mapping
    calculateWorkerIdToTargets();

    // create the delegate
    this.delegate = new ChannelDataFlowOperation(channel);
  }

  private void calculateWorkerIdToTargets() {
    for (int t : targets) {
      int worker = taskPlan.getExecutorForChannel(t);
      List<Integer> ts;
      if (workerToTargets.containsKey(worker)) {
        ts = workerToTargets.get(worker);
      } else {
        ts = new ArrayList<>();
      }
      ts.add(t);
      workerToTargets.put(worker, ts);
      targetsToWorkers.put(t, worker);
    }
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    return false;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return false;
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return merger.onMessage(source, 0, target, flags, message);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    List<Object> messages = merged.get(target);
    if (message == null) {
      messages = new ArrayList<>();
      merged.put(target, messages);
    }

    if (message instanceof List) {
      messages.addAll((Collection<?>) message);
    } else {
      messages.add(message);
    }

    return true;
  }

  @Override
  public boolean progress() {
    List<Integer> targets = workerToTargets.get(nextWorker);

    for (int i = targetIndex; i < targets.size(); i++) {
      delegate.sendMessage(0, null, targets.get(i), 0, new RoutingParameters());
    }

    // now set the things
    return OperationUtils.progressReceivers(delegate, lock, finalReceiver, partialLock, merger);
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
  public void clean() {
    if (merged != null) {
      merger.clean();
    }

    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public TaskPlan getTaskPlan() {
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
    return false;
  }

  @Override
  public boolean receiveSendInternally(int source, int target, int path,

                                       int flags, Object message) {
    return false;
  }
}
