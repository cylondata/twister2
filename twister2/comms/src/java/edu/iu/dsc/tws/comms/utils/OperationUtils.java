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
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelDataFlowOperation;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;

public final class OperationUtils {
  private static final Logger LOG = Logger.getLogger(OperationUtils.class.getName());

  private OperationUtils() {
  }

  /**
   * Progress the receivers and return true if needs further progress
   * @param finalLock lock for final receiver
   * @param finalReceiver final receiver
   * @param partialLock lock for partial receiver
   * @param partialReceiver partial receiver
   * @return true if need further progress
   */
  public static boolean areReceiversComplete(Lock finalLock, MessageReceiver finalReceiver,
                                             Lock partialLock, MessageReceiver partialReceiver) {
    boolean finalComplete = false;
    boolean mergeComplete = false;
    try {
      if (finalLock.tryLock()) {
        try {
          finalComplete = finalReceiver.isComplete();
        } finally {
          finalLock.unlock();
        }
      }

      if (partialLock.tryLock()) {
        try {
          mergeComplete = partialReceiver.isComplete();
        } finally {
          partialLock.unlock();
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
    return finalComplete && mergeComplete;
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
    return finalNeedsProgress || partialNeedsProgress;
  }

  public static Map<Integer, List<Integer>> getIntegerListMap(InvertedBinaryTreeRouter router,
                                                               LogicalPlan instancePlan,
                                                               int destination) {
    Map<Integer, List<Integer>> integerMapMap = router.receiveExpectedTaskIds();
    // add the main task to receive from iteself
    int key = router.mainTaskOfExecutor(instancePlan.getThisWorker(),
        CommunicationContext.DEFAULT_DESTINATION);
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

  public static void shuffleArray(Random random, int[] array) {
    int count = array.length;
    for (int i = count; i > 1; i--) {
      swap(array, i - 1, random.nextInt(i));
    }
  }

  public static void swap(int[] array, int i, int j) {
    int temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  public static String printStackTrace(StackTraceElement[] elements) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < elements.length; i++) {
      StackTraceElement s = elements[i];
      sb.append("\tat " + s.getClassName() + "." + s.getMethodName()
          + "(" + s.getFileName() + ":" + s.getLineNumber() + ")" + "\n");
    }
    return sb.toString();
  }
}
