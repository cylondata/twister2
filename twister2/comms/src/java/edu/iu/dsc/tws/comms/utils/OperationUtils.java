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
package edu.iu.dsc.tws.comms.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.ChannelDataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;

public final class OperationUtils {
  private static final Logger LOG = Logger.getLogger(OperationUtils.class.getName());

  private OperationUtils() {
  }

  /**
   * Progress the receivers and return true if needs further progress
   * @param delegate the channel dataflow opeation
   * @param lock lock for final receiver
   * @param finalReceiver final receiver
   * @param partialLock lock for partial receiver
   * @param partialReceiver partial receiver
   * @return true if need further progress
   */
  public static boolean progressReceivers(ChannelDataFlowOperation delegate, Lock lock,
                                       MessageReceiver finalReceiver, Lock partialLock,
                                       MessageReceiver partialReceiver) {
    boolean finalNeedsProgress = false;
    boolean partialNeedsProgress = false;
    try {
      delegate.progress();
      if (lock.tryLock()) {
        try {
          finalNeedsProgress = finalReceiver.progress();
        } finally {
          lock.unlock();
        }
      }

      if (partialLock.tryLock()) {
        try {
          partialNeedsProgress = partialReceiver.progress();
        } finally {
          partialLock.unlock();
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
//    LOG.info("Receivers: " + finalNeedsProgress + " " + partialNeedsProgress);
    return finalNeedsProgress || partialNeedsProgress;
  }

  public static Map<Integer, List<Integer>> getIntegerListMap(InvertedBinaryTreeRouter router,
                                                               TaskPlan instancePlan,
                                                               int destination) {
    Map<Integer, List<Integer>> integerMapMap = router.receiveExpectedTaskIds();
    // add the main task to receive from iteself
    int key = router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
        DataFlowContext.DEFAULT_DESTINATION);
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
}
