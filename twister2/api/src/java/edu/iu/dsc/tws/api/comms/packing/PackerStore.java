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
package edu.iu.dsc.tws.api.comms.packing;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * <p>When generating sending buffers, we need to first append the length of the
 * message as a header. In order to do that, packers might have to serialize
 * data first, so engine can retrieve the length via
 * {@link DataPacker#determineLength(Object, PackerStore)}. Twister2 will pass
 * {@link PackerStore} to this method, so packer can store the serialized data
 * temporarily to store and use later when writing to the buffer when
 * {@link DataPacker#writeDataToBuffer(Object, PackerStore, int, int, int, ByteBuffer)} is called by engine.
 * <p>
 * <p>
 * Additionally, user can consider {@link PackerStore} as an instance of {@link java.util.Map},
 * where arbitrary key,value pairs can be persisted. These values will be automatically cleared
 * by the engine when the 'current object' is fully serialized.
 * </p>
 */
public abstract class PackerStore extends HashMap<String, Object> {

  /**
   * Stores the serialized data temporarily
   */
  public abstract void store(byte[] data);

  /**
   * Retrieve the stored serialized message
   */
  public abstract byte[] retrieve();
}
