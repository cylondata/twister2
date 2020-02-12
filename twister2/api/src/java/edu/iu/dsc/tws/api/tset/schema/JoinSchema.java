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


public class JoinSchema extends KeyedSchema {
  private MessageType dTypeR;
  private int rightDatSize;

  public JoinSchema(MessageType keyType, MessageType leftDataType,
                    MessageType rightDataType) {
    super(keyType, leftDataType);
    this.dTypeR = rightDataType;
  }

  public JoinSchema(MessageType keyType, MessageType leftDataType,
                    MessageType rightDataType, int keySize, int leftDataSize, int rightDataSize) {
    super(keyType, leftDataType, keySize, leftDataSize);
    this.dTypeR = rightDataType;
    this.rightDatSize = rightDataSize;
  }

  public JoinSchema(TupleSchema leftSchema, TupleSchema rightSchema) {
    this(leftSchema.getKeyType(), leftSchema.getDataType(),
        rightSchema.getDataType());
    if (leftSchema.getClass() != rightSchema.getClass()) {
      throw new RuntimeException("Left and right schemas have different key "
          + "types");
    }
  }

  public int getRightDatSize() {
    return rightDatSize;
  }

  public boolean isRightLengthsSpecified() {
    return this.getKeySize() != -1 && this.rightDatSize != -1;
  }

  public int getRightTotalSize() {
    if (!this.isRightLengthsSpecified()) {
      return -1;
    }
    return this.getKeySize() + this.getRightDatSize();
  }

  public MessageType getDataTypeRight() {
    return dTypeR;
  }
}
