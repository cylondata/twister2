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

package org.apache.storm.tuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class Fields implements Iterable<String>, Serializable {
  private static final long serialVersionUID = 4882556192519443356L;

  public Fields(String... fields) {

  }

  public Fields(List<String> fields) {

  }

  public List<Object> select(Fields selector, List<Object> tuple) {
    return null;
  }

  public List<String> toList() {
    return null;
  }

  public int size() {
    return 0;
  }

  public String get(int index) {
    return null;
  }

  public Iterator<String> iterator() {
    return null;
  }

  /**
   * Returns the position of the specified field.
   */
  public int fieldIndex(String field) {
    return 0;
  }

  /**
   * Returns true if this contains the specified name of the field.
   */
  public boolean contains(String field) {
    return false;
  }

  public String toString() {
    return null;
  }
}