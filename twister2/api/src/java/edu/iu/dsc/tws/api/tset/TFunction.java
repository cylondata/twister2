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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all the functions in TSet implementation
 */
public interface TFunction extends Serializable {

  Map<String, Object> INPUT_MAP = new HashMap<>();

  /**
   * Prepare the function
   *
   * @param context context
   */
  default void prepare(TSetContext context) {
    INPUT_MAP.putAll(context.getInputMap());
  }

  /**
   * Gets the input value for the given key from the input map
   *
   * @param key the key to be retrieved
   * @return the object associated with the given key, null if the key is not present
   */
  default Object getInput(String key) {
    return INPUT_MAP.get(key);
  }
}
