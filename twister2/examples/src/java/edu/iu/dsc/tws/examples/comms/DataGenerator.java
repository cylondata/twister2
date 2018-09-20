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
package edu.iu.dsc.tws.examples.comms;

import java.util.Random;

public final class DataGenerator {
  private DataGenerator() {
  }

  public static int[] generateIntData(int size) {
    int[] d = new int[size];
    for (int i = 0; i < size; i++) {
      d[i] = i;
    }
    return d;
  }

  public static byte[] generateByteData(int size) {
    byte[] b = new byte[size];
    new Random().nextBytes(b);
    return b;
  }

  public static int[] generateIntData(int size, int value) {
    int[] b = new int[size];
    for (int i = 0; i < size; i++) {
      b[i] = value;
    }
    return b;
  }

  public static double[] generateDoubleData(int size) {
    double[] b = new double[size];
    for (int i = 0; i < size; i++) {
      b[i] = 1;
    }
    return b;
  }
}
