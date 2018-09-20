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
package edu.iu.dsc.tws.examples.batch.sort;

import java.util.Objects;
import java.util.Random;

public class Record implements Comparable<Record> {
  private int key;

  private byte[] data;

  public Record(int key, int size) {
    Random random = new Random();
    this.key = key;
    data = new byte[size];
    random.nextBytes(data);
  }

  public int getKey() {
    return key;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record record = (Record) o;
    return key == record.key;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }


  @Override
  public int compareTo(Record o) {
    return Integer.compare(o.key, key);
  }
}
