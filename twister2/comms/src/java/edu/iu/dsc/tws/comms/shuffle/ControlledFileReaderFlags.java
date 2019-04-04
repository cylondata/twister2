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
package edu.iu.dsc.tws.comms.shuffle;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Memory mappable limit can be increased in linux with following command.
 * sysctl -w vm.max_map_count=files_count
 */
public class ControlledFileReaderFlags {

  private long amountLoadedToMemory;
  private long softMemoryLimit;

  //http://www.mapdb.org/blog/mmap_files_alloc_and_jvm_crash/
  private long memMapLimit;
  private long memMappedCount = 0;

  private PriorityQueue<ControlledFileReader> invertedMemMapQueue;

  public ControlledFileReaderFlags(long softMemoryLimit, Comparator keyComparator) {
    //mmap : using just half of what's available
    this(softMemoryLimit, 32000, keyComparator);
  }

  /**
   * This constructor can be used to configure an instance with an arbitrary amount of
   * mem maps. Currently we are not using this though.
   */
  public ControlledFileReaderFlags(long softMemoryLimit,
                                   long memMapLimit,
                                   Comparator keyComparator) {
    this.softMemoryLimit = softMemoryLimit;
    this.memMapLimit = memMapLimit;
    this.invertedMemMapQueue = new PriorityQueue<>(
        (o1, o2) -> {
          if (o1.nextKey() == null && o2.nextKey() == null) {
            return 0;
          } else if (o1.nextKey() == null) {
            return -1;
          } else if (o2.nextKey() == null) {
            return 1;
          } else {
            return keyComparator.compare(o1.nextKey(), o2.nextKey()) * -1;
          }
        }
    );
  }

  public boolean hasMemoryLimitReached() {
    return this.amountLoadedToMemory > this.softMemoryLimit;
  }

  public void increaseMemoryLoad(int amount) {
    this.amountLoadedToMemory += amount;
  }

  public void decreaseMemoryLoad(int amount) {
    this.amountLoadedToMemory -= amount;
  }

  public void increaseMemMapLoad(ControlledFileReader reader) {
    this.memMappedCount++;
    if (this.hasMemMapLimitReached()) {
      this.cleanMemoryMaps();
    }
    this.invertedMemMapQueue.add(reader);
  }

  private void cleanMemoryMaps() {
    while (this.memMappedCount > this.memMapLimit / 2) {
      ControlledFileReader poll = this.invertedMemMapQueue.poll();
      if (poll != null) {
        poll.releaseResources();
      }
      this.memMappedCount--;
    }
  }

  public boolean hasMemMapLimitReached() {
    return this.memMappedCount > this.memMapLimit;
  }
}
