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

package edu.iu.dsc.tws.api.comms.messaging.types;

import java.lang.reflect.Array;
import java.util.Objects;

import edu.iu.dsc.tws.api.comms.packing.DataPacker;

public final class PrimitiveMessageTypes<T> implements MessageType<T, T> {

  private boolean isPrimitive;
  private int size;
  private Class<T> clazz;
  private DataPacker<T, T> dataPacker;
  private boolean isArray = false;

  public PrimitiveMessageTypes(boolean primitive, int size, Class<T> clazz,
                               DataPacker<T, T> dataPacker) {
    this.isPrimitive = primitive;
    this.size = size;
    this.clazz = clazz;
    this.dataPacker = dataPacker;
  }

  public PrimitiveMessageTypes(boolean primitive, int size, Class<T> clazz,
                               DataPacker<T, T> dataPacker, boolean isArray) {
    this.isPrimitive = primitive;
    this.size = size;
    this.clazz = clazz;
    this.dataPacker = dataPacker;
    this.isArray = isArray;
  }

  /**
   * Checks if the given message type is of a primitive type
   * if the type is primitive then we do not need to add data length to the data buffers
   */
  public boolean isPrimitive() {
    return isPrimitive;
  }

  @Override
  public int getUnitSizeInBytes() {
    return this.size;
  }

  @Override
  public int getDataSizeInBytes(T data) {
    if (!this.isArray()) {
      return this.getUnitSizeInBytes();
    } else {
      return Array.getLength(data) * this.getUnitSizeInBytes();
    }
  }

  /**
   * Specify a custom data packer
   *
   * @return a custom data packer
   */
  public DataPacker<T, T> getDataPacker() {
    return dataPacker;
  }

  @Override
  public boolean isArray() {
    return this.isArray;
  }

  public Class<T> getClazz() {
    return clazz;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveMessageTypes<?> that = (PrimitiveMessageTypes<?>) o;
    return isPrimitive == that.isPrimitive
        && size == that.size
        && isArray == that.isArray
        && Objects.equals(clazz, that.clazz);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isPrimitive, size, clazz, dataPacker, isArray);
  }
}
