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

import java.util.Objects;

/**
 * A joined tuple
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

  public static <K, L, R> JoinedTuple of(K key, L left, R right) {
    return new JoinedTuple<>(key, left, right);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinedTuple<?, ?, ?> that = (JoinedTuple<?, ?, ?>) o;
    return Objects.equals(key, that.key)
        && Objects.equals(leftValue, that.leftValue)
        && Objects.equals(rightValue, that.rightValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, leftValue, rightValue);
  }

  @Override
  public String toString() {
    return "JoinedTuple{"
        + "key=" + key
        + ", leftValue=" + leftValue
        + ", rightValue=" + rightValue
        + '}';
  }
}
