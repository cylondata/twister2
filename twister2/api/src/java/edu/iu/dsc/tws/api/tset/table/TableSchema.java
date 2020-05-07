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
package edu.iu.dsc.tws.api.tset.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.tset.schema.Schema;

public class TableSchema implements Schema {
  private List<Field> types;

  public static TableSchema make(Field... t) {
    return new TableSchema(new ArrayList<>(Arrays.<Field>asList(t)));
  }

  public TableSchema(List<Field> types) {
    this.types = types;
  }

  public MessageType get(int column) {
    return types.get(column).getType();
  }

  public int getNumberOfColumns() {
    return types.size();
  }

  public List<Field> getFields() {
    return new ArrayList<>(types);
  }

  @Override
  public MessageType getDataType() {
    return MessageTypes.ARROW_TABLE;
  }

  @Override
  public int getDataSize() {
    return 0;
  }

  @Override
  public int getTotalSize() {
    return 0;
  }

  @Override
  public boolean isLengthsSpecified() {
    return false;
  }
}
