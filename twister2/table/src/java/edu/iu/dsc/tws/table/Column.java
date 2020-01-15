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
package edu.iu.dsc.tws.table;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;

/**
 * Represent the metadata about a column (attribute)
 */
public class Column {
  /**
   * The schema of the column
   */
  private MessageSchema schema;

  /**
   * The data type
   */
  private MessageType dataType;

  public Column(MessageSchema schema, MessageType dataType) {
    this.schema = schema;
    this.dataType = dataType;
  }

  public MessageSchema getSchema() {
    return schema;
  }

  public MessageType getDataType() {
    return dataType;
  }
}
