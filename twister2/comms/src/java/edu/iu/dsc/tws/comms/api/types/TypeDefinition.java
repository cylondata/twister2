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
package edu.iu.dsc.tws.comms.api.types;

import java.util.Comparator;

import edu.iu.dsc.tws.comms.api.DataPacker;

public interface TypeDefinition<T> {

  /**
   * All primitive data types should return true, including the boxed types of primitives
   *
   * @return true if this type is primitive, false otherwise
   */
  boolean isPrimitive();

  /**
   * For arrays this method should return the size of an element in bytes
   *
   * @return the size of an unit.
   */
  int getUnitSizeInBytes();

  int getDataSizeInBytes(T data);

  Class<T> getClazz();

  DataPacker<T> getDataPacker();

  boolean isArray();

  default T cast(Object data) {
    return this.getClazz().cast(data);
  }

  default Comparator<T> getDefaultComparator() {
    throw new RuntimeException("Comparator is not defined for this type");
  }
}
