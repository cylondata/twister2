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

public class KeyValue implements Comparable<KeyValue> {
  private Object key;

  private Object value;

  private Comparator<Object> keyComparator;

  public KeyValue(Object k, Object v) {
    this.key = k;
    this.value = v;
  }

  public KeyValue(Object k, Object v, Comparator<Object> kComparator) {
    this.key = k;
    this.value = v;
    this.keyComparator = kComparator;
  }

  public Object getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public int compareTo(KeyValue o) {
    if (keyComparator == null) {
      return -1;
    }

    if (o == null) {
      throw new RuntimeException("o null");
    }
    return keyComparator.compare(this.getKey(), o.getKey());
  }
}
