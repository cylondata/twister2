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
package edu.iu.dsc.tws.examples.batch.sortop;

import java.util.Arrays;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;

public class ByteSelector implements DestinationSelector {
  protected int keysToOneTask;

  protected int[] destinationsList;

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    int totalPossibilities = 256 * 256; //considering only most significant bytes of array
    this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
    this.destinationsList = new int[destinations.size()];
    int index = 0;
    for (int i : destinations) {
      destinationsList[index++] = i;
    }
    Arrays.sort(this.destinationsList);
  }

  private int getIndex(byte[] array) {
    int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
    return key / keysToOneTask;
  }

  @Override
  public int next(int source, Object data) {
    return this.destinationsList[this.getIndex((byte[]) data)];
  }

  @Override
  public int next(int source, Object key, Object data) {
    return this.destinationsList[this.getIndex((byte[]) key)];
  }

  @Override
  public void commit(int source, int obtained) {
  }
}
