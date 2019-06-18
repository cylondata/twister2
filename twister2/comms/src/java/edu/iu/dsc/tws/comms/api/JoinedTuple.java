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
package edu.iu.dsc.tws.comms.api;

/**
 * A joined tuple
 *
 * @param <K>
 * @param <L>
 * @param <R>
 */
public class JoinedTuple<K, L, R> {
  /**
   * The key
   */
  private K key;

  /**
   * Value from left relation
   */
  private L leftValue;

  /**
   * Value from right relation
   */
  private R rightValue;

  public JoinedTuple(K key, L left, R right) {
    this.key = key;
    this.leftValue = left;
    this.rightValue = right;
  }

  public K getKey() {
    return key;
  }

  public L getLeftValue() {
    return leftValue;
  }

  public R getRightValue() {
    return rightValue;
  }
}
