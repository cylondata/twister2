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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class PartitionBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionBatchFinalReceiver.class.getName());

  /**
   * The receiver to be used to deliver the message
   */
  private BulkReceiver receiver;

  /**
   * The executor
   */
  protected int executor;

  /**
   * Keep the destination messages
   */
  private Map<Integer, List<Object>> targetMessages = new HashMap<>();

  /**
   * The dataflow operation
   */
  private DataFlowOperation operation;

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  private Lock lock = new ReentrantLock();

  /**
   * These sources called onFinished
   */
  private Set<Integer> onFinishedSources = new HashSet<>();

  /**
   * Sources of this worker
   */
  private Set<Integer> thisWorkerSources = new HashSet<>();

  /**
   * The worker id
   */
  private int thisWorker;

  public PartitionBatchFinalReceiver(BulkReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    TaskPlan taskPlan = op.getTaskPlan();
    thisWorkerSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan,
        ((DataFlowPartition) op).getSources());
    thisWorker = op.getTaskPlan().getThisExecutor();
    this.operation = op;

    // lists to keep track of messages for destinations
    for (int d : expectedIds.keySet()) {
      targetMessages.put(d, new ArrayList<>());
    }
  }

  /**
   * All message that come to the partial receiver are handled by this method. Since we currently
   * do not have a need to know the exact source at the receiving end for the parition operation
   * this method uses a representative source that is used when forwarding the message to its true
   * target
   *
   * @param src the source of the message
   * @param path the path that is taken by the message, that is intermediate targets
   * @param target the target of this receiver
   * @param flags the communication flags
   * @param object the actual message
   * @return true if the message was successfully forwarded or queued.
   */
  @Override
  public boolean onMessage(int src, int path, int target, int flags, Object object) {
    lock.lock();

    if ((flags & MessageFlags.END) == MessageFlags.END) {
      if (onFinishedSources.contains(src)) {
        LOG.log(Level.WARNING,
            String.format("%d Duplicate finish from source id %d", this.thisWorker, src));
      } else {
        onFinishedSources.add(src);
      }
      return true;
    }

    try {
      List<Object> targetMsgList = targetMessages.get(target);
      if (object instanceof List) {
        targetMsgList.addAll((Collection<?>) object);
      } else {
        targetMsgList.add(object);
      }
    } finally {
      lock.unlock();
    }
    return true;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    lock.lock();
    try {
      if (operation.isDelegeteComplete()
          && onFinishedSources.equals(thisWorkerSources)) {
        Iterator<Map.Entry<Integer, List<Object>>> it = targetMessages.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Integer, List<Object>> e = it.next();
          if (receiver.receive(e.getKey(), e.getValue().iterator())) {
            it.remove();
          } else {
            needsFurtherProgress = true;
          }
        }
      } else {
        needsFurtherProgress = true;
      }
    } finally {
      lock.unlock();
    }
    return needsFurtherProgress;
  }
}
