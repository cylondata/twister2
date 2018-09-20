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
import java.util.Comparator;

import edu.iu.dsc.tws.comms.shuffle.KeyValue;

public class Heap<K, V> {

  private ArrayList<HeapNode<K, V>> heapArr = new ArrayList<>();
  private Comparator<K> keyComparator;

  public Heap(int k, Comparator<K> kComparator) {
    this.keyComparator = kComparator;
    this.heapArr.ensureCapacity(k);
  }

  public Heap(Comparator<K> kComparator) {
    this.keyComparator = kComparator;
  }

  public void insert(KeyValue<K, V> data, int listNo) {
    this.heapArr.add(new HeapNode<>(data, listNo, this.keyComparator));
    this.bubbleUp();
  }

  public HeapNode<K, V> extractMin() {
    if (heapArr.isEmpty()) {
      return null;
    }

    // extract the root
    HeapNode<K, V> min = heapArr.remove(0);
    // replace the root with the last element in the heap
    if (heapArr.size() > 1) {
      heapArr.add(0, heapArr.remove(heapArr.size() - 1));
      // sink down the root to its correct position
      sinkDown(0);
    }

    return min;
  }

  private void sinkDown(int k) {
    int smallest = k;
    // check which is smaller child , 2k or 2k+1.
    int leftChildIndex = smallest * 2 + 1;
    int rightChildIndex = smallest * 2 + 2;

    if (leftChildIndex < heapArr.size()
        && heapArr.get(leftChildIndex).compareTo(heapArr.get(smallest)) < 0) {
      smallest = leftChildIndex;
    }

    if (rightChildIndex < heapArr.size()
        && heapArr.get(rightChildIndex).compareTo(heapArr.get(smallest)) < 0) {
      smallest = rightChildIndex;
    }

    if (smallest != k) { // if any if the child is small, swap
      swap(k, smallest);
      sinkDown(smallest); // call recursively
    }
  }

  private void swap(int a, int b) {
    HeapNode<K, V> temp = heapArr.get(a);
    heapArr.set(a, heapArr.get(b));
    heapArr.set(b, temp);
  }

  private void bubbleUp() {
    // last position
    int pos = this.heapArr.size() - 1;

    // check if its parent is greater.
    while (pos > 0
        && heapArr.get((pos - 1) / 2).compareTo(heapArr.get(pos)) > 0) {
      this.swap((pos - 1) / 2, pos);
      pos = (pos - 1) / 2; // make pos to its parent for next iteration.
    }
  }

  /**
   * Returning raw array for testing purposes
   */
  public ArrayList<HeapNode<K, V>> getRawArray() {
    return heapArr;
  }

  @Override
  public String toString() {
    return "Heap{"
        + "heap="
        + heapArr
        + '}';
  }
}
