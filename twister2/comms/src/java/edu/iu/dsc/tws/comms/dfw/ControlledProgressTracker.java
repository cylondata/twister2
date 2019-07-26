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
import java.util.concurrent.ArrayBlockingQueue;

public class ControlledProgressTracker {
  /**
   * The actual items
   */
  private List<Queue<Integer>> progressItems;

  private boolean canProgress;

  private Queue<Integer> currentQueue;

  /**
   * Mapping from taskId -> index
   */
  private Map<Integer, Integer> invertedItems = new HashMap<>();

  public ControlledProgressTracker(List<List<Integer>> items) {
    if (items.size() == 0) {
      canProgress = false;
    } else {
      canProgress = true;
      this.progressItems = new ArrayList<>(items.size());
      for (int i = 0; i < items.size(); i++) {
        List<Integer> taskLists = items.get(i);
        Queue<Integer> progressQueue = new ArrayBlockingQueue<>(taskLists.size());
        progressQueue.addAll(taskLists);
        progressItems.add(progressQueue);
        for (int t : taskLists) {
          invertedItems.put(t, i);
        }
      }
    }
  }

  public void switchGroup(int group) {
    this.currentQueue = progressItems.get(group);
  }

  /**
   * Get the next item
   * @return next
   */
  public int next() {
    if (currentQueue.size() > 0) {
      Integer next = currentQueue.poll();
      if (next == null) {
        return Integer.MIN_VALUE;
      } else {
        return next;
      }
    } else {
      return Integer.MIN_VALUE;
    }
  }

  /**
   * Add the item back to the progression
   * @param item item
   */
  public void finish(int item) {
    int index = invertedItems.get(item);
    Queue<Integer> queue = progressItems.get(index);
    boolean offer = queue.offer(item);
    if (!offer) {
      throw new RuntimeException("We should always accept");
    }
  }

  public boolean canProgress() {
    return canProgress;
  }
}
