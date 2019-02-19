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
package edu.iu.dsc.tws.comms.dfw.io.direct;

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

public class DirectBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(DirectBatchFinalReceiver.class.getName());

  /**
   * The receiver to be used to deliver the message
   */
  protected BulkReceiver receiver;

  /**
   * The executor
   */
  protected int executor;

  /**
   * Keep the destination messages
   */
  protected Map<Integer, List<Object>> targetMessages = new HashMap<>();

  /**
   * The dataflow operation
   */
  protected DataFlowOperation operation;

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  protected Lock lock = new ReentrantLock();

  /**
   * These sources called onFinished
   */
  protected Set<Integer> finishedTargets = new HashSet<>();

  /**
   * The worker id
   */
  private int thisWorker;

  /**
   * sources for this operation
   */
  private Set<Integer> sources;

  public DirectBatchFinalReceiver(BulkReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    thisWorker = op.getTaskPlan().getThisExecutor();
    this.operation = op;
    this.sources = op.getSources();

    // lists to keep track of messages for destinations
    for (int d : expectedIds.keySet()) {
      targetMessages.put(d, new ArrayList<>());
    }

    LOG.log(Level.FINE, String.format("%d Expected ids %s", executor, expectedIds));
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
    try {
      if ((flags & MessageFlags.END) == MessageFlags.END) {
        if (finishedTargets.contains(target)) {
          LOG.log(Level.WARNING,
              String.format("%d Duplicate finish to target id %d from source %d",
                  this.thisWorker, target, src));
        } else {
          finishedTargets.add(target);
        }
        return true;
      }

      List<Object> targetMsgList = targetMessages.get(target);
      if (targetMsgList == null) {
        throw new RuntimeException(String.format("%d target not exisits %d %s", executor, target,
            operation.getTaskPlan()));
      }
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
      for (int target : finishedTargets) {
        if (operation.isDelegateComplete()) {
          Iterator<Map.Entry<Integer, List<Object>>> it = targetMessages.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<Integer, List<Object>> e = it.next();
            if (e.getKey() == target) {
              if (receiver.receive(e.getKey(), e.getValue().iterator())) {
                it.remove();
              } else {
                needsFurtherProgress = true;
              }
            }
          }
        } else {
          needsFurtherProgress = true;
        }
      }
    } finally {
      lock.unlock();
    }
    return needsFurtherProgress;
  }
}
