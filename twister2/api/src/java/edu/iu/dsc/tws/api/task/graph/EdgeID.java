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
package edu.iu.dsc.tws.api.task.graph;

import java.util.concurrent.atomic.AtomicInteger;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

/**
 * When instantiating the comms, we need to assign and id for the edge.
 * <p>
 * This class will be used to handle the following issue.
 * https://github.com/DSC-SPIDAL/twister2/issues/519
 * <p>
 * EdgeID will preallocate 5 IDs per edge. When configuring a communication operation,
 * these IDs can be used without hitting the above issue.
 */
public class EdgeID {

  private static AtomicInteger currentId = new AtomicInteger(0);

  private final int startIndex;
  private int consumption = 0;

  public EdgeID() {
    this.startIndex = currentId.getAndAdd(5);
  }

  public int nextId() {
    if (consumption == 5) {
      throw new Twister2RuntimeException("No more IDs to consume.");
    }
    return startIndex + (consumption++);
  }
}
