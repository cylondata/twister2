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

package edu.iu.dsc.tws.api.tset;

import java.util.List;

import edu.iu.dsc.tws.dataset.DataObject;

/**
 * All Tsets that are cachable need to implement this interface
 * This interface defines the methods that other classes can use to
 * access the cached data
 */
public interface Cacheable<T> {

  /**
   * retrieve data saved in the TSet
   * @return dataObject
   */
  List<T> getData();

  /**
   * retrieve data saved in the TSet
   * @return dataObject
   */
  DataObject<T> getDataObject();

  /**
   * Add Data to the data object
   * @param value value to be added
   * @return true if the data was added successfully or false otherwise
   */
  boolean addData(T value);

}
