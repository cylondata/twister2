//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
///home/niranda/git/twister2/twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing/Snapshot.java
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

package edu.iu.dsc.tws.api.checkpointing;

import edu.iu.dsc.tws.api.comms.packing.DataPacker;

/**
 * Represent a snapshot persisted
 */
public interface Snapshot {

  /**
   * This method can be used to define packers. Packers will be used when deserializing
   * values. If packer is not defined for a particular value, that value will be treated
   * as an {@link Object} and serialized with {@link edu.iu.dsc.tws.api.comms.packing.types.ObjectPacker}
   * @param key key
   * @param dataPacker the data packer to use for the key
   */
  void setPacker(String key, DataPacker dataPacker);

  /**
   * This method can be used to set/update values into the snapshot
   * @param key key
   * @param value value to add
   */
  void setValue(String key, Object value);

  /**
   * Set a value along with the packer to use for serializing the value
   * @param key key
   * @param value value
   * @param packer packer
   */
  default void setValue(String key, Object value, DataPacker packer) {
    this.setPacker(key, packer);
    this.setValue(key, value);
  }

  /**
   * Get the value for key or the default
   * @param key key
   * @param defaultValue default value
   * @return value corresponding to key
   */
  Object getOrDefault(String key, Object defaultValue);

  /**
   * Get the value for a key
   * @param key key
   * @return value
   */
  Object get(String key);

  /**
   * Returns the current version of the snapshot
   */
  long getVersion();

  /**
   * Weater a checkpoint available for a kye
   * @param key key
   * @return true if a key is available
   */
  boolean checkpointAvailable(String key);
}
