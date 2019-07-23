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
package edu.iu.dsc.tws.examples.batch.terasort;

import java.util.Arrays;
import java.util.Set;

public class TaskPartitionerForSampledData extends TaskPartitionerForRandom {

  private byte[] minMax;
  private int keySize;
  private double ratio;
  private int minIndexForSample;
  private int maxIndexMaxIndexForSample;

  private int lowestTaskIndex = -1;

  public TaskPartitionerForSampledData(byte[] minMax, int keySize) {
    this.minMax = minMax;
    this.keySize = keySize;
  }

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    super.prepare(sources, destinations);
    byte[] min = Arrays.copyOfRange(minMax, 0, keySize);
    byte[] max = Arrays.copyOfRange(minMax, keySize, minMax.length);

    this.minIndexForSample = super.getIndex(min);
    this.maxIndexMaxIndexForSample = super.getIndex(max);

    this.ratio = (double) (this.destinationsList.size() - 1)
        / (maxIndexMaxIndexForSample - minIndexForSample);

    this.lowestTaskIndex = this.destinationsList.size() - 1;
  }

  @Override
  protected int getIndex(byte[] array) {
    int index = super.getIndex(array);
    if (index < this.minIndexForSample) {
      return 0;
    }

    if (index > this.maxIndexMaxIndexForSample) {
      return this.lowestTaskIndex;
    }

    return (int) ((index - minIndexForSample) * ratio);
  }

  @Override
  public int partition(int source, byte[] data) {
    return this.destinationsList.get(this.getIndex(data));
  }
}
