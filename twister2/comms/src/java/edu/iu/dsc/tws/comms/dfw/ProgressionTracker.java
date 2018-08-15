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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Class for tracking the communicationProgress of set of items
 */
public class ProgressionTracker {
  private Queue<Integer> progressItems;

  private boolean canProgress;

  public ProgressionTracker(Set<Integer> items) {
    if (items.size() == 0) {
      canProgress = false;
    } else {

      canProgress = true;
      this.progressItems = new ArrayBlockingQueue<>(items.size());
      for (int i : items) {
        progressItems.offer(i);
      }
    }
  }

  /**
   * Get the next item
   * @return next
   */
  public int next() {
    if (progressItems.size() > 0) {
      Integer next = progressItems.poll();
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
    boolean offer = progressItems.offer(item);
    if (!offer) {
      throw new RuntimeException("We should always accept");
    }
  }

  public boolean canProgress() {
    return canProgress;
  }
}
