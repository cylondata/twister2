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

package edu.iu.dsc.tws.comms.dfw.io;

import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.packing.MessageSerializer;

public class Serializers {

  private Serializers() {

  }

  public static MessageSerializer get(boolean isKeyed, MessageSchema messageSchema) {
    if (!isKeyed) {
      if (messageSchema.isFixedSchema()) {
        return new FixedSchemaDataSerializer(messageSchema);
      }
      return new DataSerializer();
    } else {
      return new KeyedDataSerializer();
    }
  }
}
