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

import java.util.Arrays;

import org.apache.commons.collections4.map.HashedMap;

public class THashMap<K, V> extends HashedMap<K, V> {


  @Override
  protected int hash(Object o) {
    if (o != null && o.getClass().isArray()) {
      if (o instanceof byte[]) {
        return Arrays.hashCode((byte[]) o);
      } else if (o instanceof int[]) {
        return Arrays.hashCode((int[]) o);
      } else if (o instanceof long[]) {
        return Arrays.hashCode((long[]) o);
      } else if (o instanceof double[]) {
        return Arrays.hashCode((double[]) o);
      } else if (o instanceof float[]) {
        return Arrays.hashCode((float[]) o);
      } else if (o instanceof short[]) {
        return Arrays.hashCode((short[]) o);
      } else if (o instanceof boolean[]) {
        return Arrays.hashCode((boolean[]) o);
      } else if (o instanceof char[]) {
        return Arrays.hashCode((char[]) o);
      } else {
        throw new UnsupportedOperationException("Array type of " + o.getClass().getSimpleName()
            + " Not currently supported");
      }
    }
    return super.hash(o);
  }

  @Override
  protected boolean isEqualKey(Object o, Object o1) {
    if (o != null && o1 != null && o.getClass().isArray() && o1.getClass().isArray()) {
      if (o instanceof byte[]) {
        return Arrays.equals((byte[]) o, (byte[]) o1);
      } else if (o instanceof int[]) {
        return Arrays.equals((int[]) o, (int[]) o1);
      } else if (o instanceof long[]) {
        return Arrays.equals((long[]) o, (long[]) o1);
      } else if (o instanceof double[]) {
        return Arrays.equals((double[]) o, (double[]) o1);
      } else if (o instanceof float[]) {
        return Arrays.equals((float[]) o, (float[]) o1);
      } else if (o instanceof short[]) {
        return Arrays.equals((short[]) o, (short[]) o1);
      } else if (o instanceof boolean[]) {
        return Arrays.equals((boolean[]) o, (boolean[]) o1);
      } else if (o instanceof char[]) {
        return Arrays.equals((char[]) o, (char[]) o1);
      } else {
        throw new UnsupportedOperationException("Array type of " + o.getClass().getSimpleName()
            + " Not currently supported");
      }
    }
    return super.isEqualKey(o, o1);
  }
}
