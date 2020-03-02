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
package edu.iu.dsc.tws.examples.batch.arrow;

import java.util.Random;

public class ArrowExampleClass {
  public int anInt;
  public long aLong;
  public byte[] arr;
  public float aFloat;
  public Random random;

  public ArrowExampleClass(Random random, int index) {
    this.random = random;
    this.anInt = this.random.nextInt(1024);
    this.aLong = this.random.nextInt(Integer.MAX_VALUE);
    this.arr = new byte[this.random.nextInt(1024)];
    this.random.nextBytes(this.arr);
    this.aFloat = this.random.nextFloat();
  }

  public static String firstX(byte[] data, int items) {
    int toProcess = Math.min(items, data.length);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < toProcess; i++) {
      sb.append(String.format("0x%02x", data[i]) + " ");
    }
    return sb.toString();
  }

  public static long hashArray(byte[] data) {
    long ret = 0;
    for (int i = 0; i < data.length; i++) {
      ret += data[i];
    }
    return ret;
  }

  public String toString() {
    return anInt + "\t | " + aLong + "\t | " + " arr[" + this.arr.length + "] "
        + firstX(this.arr, 5) + "\t | " + aFloat;
  }

  public long getSumHash() {
    long ret = 0;
    ret += anInt;
    ret += aLong;
    ret += ArrowExampleClass.hashArray(this.arr);
    ret += aFloat;
    return ret;
  }
}
