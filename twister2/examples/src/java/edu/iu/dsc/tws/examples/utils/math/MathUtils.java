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
package edu.iu.dsc.tws.examples.utils.math;

import java.util.List;

public final class MathUtils {

  private MathUtils() {
  }

  public static int[] sumList(List<int[]> messages) {
    int[] arr = null;
    if (messages != null) {
      int arrLength = messages.get(0).length;
      arr = new int[arrLength];
      for (int[] a : messages) {
        for (int i = 0; i < arrLength; i++) {
          arr[i] += a[i];
        }
      }
    } else {
      throw new NullPointerException("List is null");
    }
    return arr;
  }
}
