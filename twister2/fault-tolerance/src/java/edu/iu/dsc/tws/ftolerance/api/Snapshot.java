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

package edu.iu.dsc.tws.ftolerance.api;

import edu.iu.dsc.tws.comms.api.DataPacker;

public interface Snapshot {

  /**
   * This method can be used to define packers. Packers will be used when deserializing
   * values. If packer is not defined for a particular value, that value will be treated
   * as an {@link Object} and serialized with {@link edu.iu.dsc.tws.comms.dfw.io.types.ObjectPacker}
   */
  void setPacker(String key, DataPacker dataPacker);

  /**
   * This method can be used to set/update values into the snapshot
   */
  void setValue(String key, Object value);

  default void setValue(String key, Object value, DataPacker packer) {
    this.setPacker(key, packer);
    this.setValue(key, value);
  }

  Object getOrDefault(String key, Object defaultValue);

  Object get(String key);

  /**
   * Returns the current version of the snapshot
   */
  long getVersion();

  boolean checkpointAvailable(String key);
}
