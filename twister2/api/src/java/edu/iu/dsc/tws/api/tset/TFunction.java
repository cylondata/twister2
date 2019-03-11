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
import java.util.Map;

/**
 * Base class for all the functions in TSet implementation
 */
public interface TFunction extends Serializable {

  TSetContext CONTEXT = new TSetContext();

  /**
   * Prepare the function
   *
   * @param context CONTEXT
   */
  default void prepare(TSetContext context) {
    this.CONTEXT.setConfig(context.getConfig());
    this.CONTEXT.settSetIndex(context.gettSetIndex());
    this.CONTEXT.settSetId(context.gettSetId());
    this.CONTEXT.settSetName(context.gettSetName());
    this.CONTEXT.setParallelism(context.getParallelism());
    this.CONTEXT.setConfig(context.getConfig());
    this.CONTEXT.setWorkerId(context.getWorkerId());
    this.CONTEXT.addInputMap(context.getInputMap());
  }

  /**
   * Gets the input value for the given key from the input map
   *
   * @param key the key to be retrieved
   * @return the object associated with the given key, null if the key is not present
   */
  default Object getInput(String key) {
    return CONTEXT.getInputMap().get(key);
  }

  /**
   * Adds the given key value pair into the {@link TFunction#CONTEXT} input map
   *
   * @param key the key to be added
   * @param input the value associated with the key
   */
  default void addInput(String key, TSet<?> input) {
    CONTEXT.getInputMap().put(key, input);
  }

  /**
   * Adds the given map into the input map
   *
   * @param map map that contains key, input pairs that need to be added into
   * the {@link TFunction#CONTEXT} input map
   */
  default void addInputs(Map<String, TSet<?>> map) {
    CONTEXT.getInputMap().putAll(map);
  }
}
