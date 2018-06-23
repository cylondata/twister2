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

import java.util.Comparator;

import edu.iu.dsc.tws.comms.shuffle.KeyValue;

public class Heap {
  private HeapNode[] heap;
  private int position;
  private Comparator<Object> keyComparator;

  public Heap(int k, Comparator<Object> kComparator) {
    this.keyComparator = kComparator;
    // size + 1 because index 0 will be empty
    heap = new HeapNode[k + 1];
    position = 0;
    // put some junk values at 0th index node
    heap[0] = new HeapNode(new KeyValue(new int[]{0}, ""), -1);
  }

  public void insert(KeyValue data, int listNo) {
    int[] key = (int[]) data.getKey();
    try {
      int i = key[0];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw e;
    }
    // check if Heap is empty
    if (position == 0) {
      // insert the first element in heap
      heap[position + 1] = new HeapNode(data, listNo);
      position = 2;
    } else {
      // insert the element to the end
      heap[position++] = new HeapNode(data, listNo);
      bubbleUp();
    }
  }

  public HeapNode extractMin() {
    // extract the root
    HeapNode min = heap[1];
    // replace the root with the last element in the heap
    heap[1] = heap[position - 1];
    // set the last Node as NULL
    heap[position - 1] = null;
    // reduce the position pointer
    position--;
    // sink down the root to its correct position
    sinkDown(1);
    return min;
  }

  private void sinkDown(int k) {
    int smallest = k;
    // check which is smaller child , 2k or 2k+1.
    if (2 * k < position
        && keyComparator.compare(heap[smallest].data.getKey(), heap[2 * k].data.getKey()) > 0)  {
      smallest = 2 * k;
    }
    if (2 * k + 1 < position
        && keyComparator.compare(heap[smallest].data.getKey(), heap[2 * k + 1].data.getKey()) > 0) {
      smallest = 2 * k + 1;
    }
    if (smallest != k) { // if any if the child is small, swap
      swap(k, smallest);
      sinkDown(smallest); // call recursively
    }
  }

  private void swap(int a, int b) {
    // System.out.println("swappinh" + mH[a] + " and " + mH[b]);
    HeapNode temp = heap[a];
    heap[a] = heap[b];
    heap[b] = temp;
  }

  private void bubbleUp() {
    // last position
    int pos = position - 1;
    // check if its parent is greater.
    while (pos > 0
        && keyComparator.compare(heap[pos / 2].data.getKey(), heap[pos].data.getKey()) > 0) {
      HeapNode y = heap[pos]; // if yes, then swap
      heap[pos] = heap[pos / 2];
      heap[pos / 2] = y;
      pos = pos / 2; // make pos to its parent for next iteration.
    }
  }
}
