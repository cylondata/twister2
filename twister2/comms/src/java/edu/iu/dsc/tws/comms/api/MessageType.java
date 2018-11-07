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
package edu.iu.dsc.tws.comms.api;

//Note: is a new type is added make sure to update the MessageTypeUtils and
// to update edu.iu.dsc.tws.data.memory.utils.DataMessageType
// todo above comment might be invalid
public enum MessageType {

  INTEGER(true),
  CHAR(true),
  BYTE,
  MULTI_FIXED_BYTE(false, true),
  STRING,
  LONG(true),
  DOUBLE(true),
  OBJECT,
  BUFFER,
  EMPTY,
  SHORT(true);

  private boolean isMultiMessageType;
  private boolean isPrimitive;

  MessageType() {
    this(false, false);
  }

  MessageType(boolean isPrimitive) {
    this(isPrimitive, false);
  }


  MessageType(boolean isPrimitive, boolean isMultiMessageType) {
    this.isPrimitive = isPrimitive;
    this.isMultiMessageType = isMultiMessageType;
  }

  /**
   * Checks if the given message type is of a primitive type
   * if the type is primitive then we do not need to add data length to the data buffers
   */
  public boolean isPrimitive() {
    return isPrimitive;
  }

  /**
   * checks if the type is a multi message, not to be confused with the aggregated multi-messages
   * that are passed through the network when optimized communications such as reduce are performed
   * this refers to the original type of the message
   */
  public boolean isMultiMessageType() {
    return this.isMultiMessageType;
  }
}
