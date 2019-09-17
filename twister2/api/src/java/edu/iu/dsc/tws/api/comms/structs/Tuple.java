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
package edu.iu.dsc.tws.api.comms.structs;

/**
 * Keyed content is serialized given priority and serialized as two parts of key and object.
 */
public class Tuple<K, V> {
  /**
   * First value
   */
  private K key;

  /**
   * Second value
   */
  private V value;

  /**
   * Create a tuple
   */
  public Tuple() {
  }

  /**
   * Create a tuple wit key and value
   * @param key first value
   * @param value second value
   */
  public Tuple(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <K, V> Tuple of(K key, V value) {
    return new Tuple<>(key, value);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "KeyValue{"
        + "key="
        + key
        + ", value="
        + value
        + '}';
  }
}
