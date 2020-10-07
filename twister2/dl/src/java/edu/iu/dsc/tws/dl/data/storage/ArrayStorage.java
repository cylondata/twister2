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
package edu.iu.dsc.tws.dl.data.storage;

import java.util.Arrays;
import java.util.Iterator;

import edu.iu.dsc.tws.dl.data.Storage;

public class ArrayStorage<T> implements Storage<T> {
  private T[] values;

  public ArrayStorage(T[] values) {
    this.values = values;
  }

  @Override
  public int length() {
    return values.length;
  }

  @Override
  public T apply(int index) {
    return values[index];
  }

  @Override
  public void update(int index, T value) {
    values[index] = value;
  }

  @Override
  public Storage<T> copy(Storage<T> source, int offset, int sourceOffset, int length) {
    System.arraycopy(source.array(), sourceOffset, this.values, offset, length);
    return this;
  }

  @Override
  public Storage<T> fill(T value, int offset, int length) {
    return null;
  }

  @Override
  public Storage<T> resize(int size) {
    values = Arrays.copyOf(values, size);
    return this;
  }

  @Override
  public T[] array() {
    return values;
  }

  @Override
  public Storage<T> set(Storage<T> other) {
    values = other.array();
    return this;
  }

  @Override
  public Iterator<T> iterator() {
    return Arrays.stream(values).iterator();
  }
}
