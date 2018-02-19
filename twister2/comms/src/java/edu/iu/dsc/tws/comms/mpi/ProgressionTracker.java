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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ProgressionTracker {
  private Queue<Integer> progressItems;

  public ProgressionTracker(int[] items) {
    this.progressItems = new ArrayBlockingQueue<>(items.length);
    for (int i : items) {
      progressItems.offer(i);
    }
  }

  public int next() {
    if (progressItems.size() > 0) {
      return progressItems.poll();
    } else {
      return -1;
    }
  }

  public void finish(int item) {
    progressItems.offer(item);
  }
}
