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
package edu.iu.dsc.tws.api.tset.schema;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;

public class KeyedSchema implements TupleSchema {

  private final MessageType dType;
  private final MessageType kType;

  private int keySize = -1;
  private int totalSize = -1;

  public KeyedSchema(MessageType keyType, MessageType dataType) {
    this.dType = dataType;
    this.kType = keyType;
  }

  public KeyedSchema(MessageType dType, MessageType kType, int keySize, int totalSize) {
    this.dType = dType;
    this.kType = kType;
    this.keySize = keySize;
    this.totalSize = totalSize;
  }

  @Override
  public MessageType getDataType() {
    return dType;
  }

  @Override
  public int getTotalSize() {
    return this.totalSize;
  }

  public MessageType getKeyType() {
    return kType;
  }

  @Override
  public int getKeySize() {
    return this.keySize;
  }
}
