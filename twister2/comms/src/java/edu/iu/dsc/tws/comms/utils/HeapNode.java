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

import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public class HeapNode<K, V> implements Comparable<HeapNode<K, V>> {

  public Tuple<K, V> data;
  public int listNo;
  private Comparator<K> keyComparator;

  public HeapNode(Tuple<K, V> data, int listNo, Comparator<K> keyComparator) {
    this.data = data;
    this.listNo = listNo;
    this.keyComparator = keyComparator;
  }

  public Tuple<K, V> getData() {
    return data;
  }

  /**
   * Since KeyValue has been wrapped by this class, providing an easy to access method to get key
   */
  @SuppressWarnings("Unchecked")
  public K getKey() {
    return (K) this.data.getKey();
  }

  /**
   * Since KeyValue has been wrapped by this class, providing and easy to access methods to get value
   */
  @SuppressWarnings("Unchecked")
  public V getValue() {
    return (V) this.data.getValue();
  }

  @Override
  public String toString() {
    return "HeapNode{"
        + "data="
        + data
        + ", listNo="
        + listNo
        + '}';
  }

  @SuppressWarnings("Unchecked")
  @Override
  public int compareTo(HeapNode<K, V> o) {
    return this.keyComparator.compare((K) this.data.getKey(), (K) o.getData().getKey());
  }
}
