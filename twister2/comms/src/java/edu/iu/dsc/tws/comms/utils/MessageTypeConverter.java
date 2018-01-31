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
package edu.iu.dsc.tws.comms.utils;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;

/**
 * Convert between edu.iu.dsc.tws.comms.api.DataMessageType and
 * edu.iu.dsc.tws.data.memory.utils.DataMessageType
 */
public final class MessageTypeConverter {

  private MessageTypeConverter() {
  }

  public static DataMessageType toDataMessageType(MessageType a) {
    switch (a) {
      case INTEGER:
        return DataMessageType.INTEGER;
      case CHAR:
        return DataMessageType.CHAR;
      case BYTE:
        return DataMessageType.BYTE;
      case STRING:
        return DataMessageType.STRING;
      case LONG:
        return DataMessageType.LONG;
      case DOUBLE:
        return DataMessageType.DOUBLE;
      case OBJECT:
        return DataMessageType.DOUBLE;
      case BUFFER:
        return DataMessageType.BUFFER;
      case EMPTY:
        return DataMessageType.EMPTY;
      case SHORT:
        return DataMessageType.SHORT;
      default:
        throw new RuntimeException("The given Message type does not have a corresponding"
            + " DataMessageType");
    }
  }

}
