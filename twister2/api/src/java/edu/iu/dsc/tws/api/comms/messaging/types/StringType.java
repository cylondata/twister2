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

package edu.iu.dsc.tws.api.comms.messaging.types;

import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.types.StringPacker;

public class StringType implements MessageType<String, char[]> {

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public int getUnitSizeInBytes() {
    return Character.SIZE;
  }

  @Override
  public int getDataSizeInBytes(String data) {
    return data.length() * this.getUnitSizeInBytes();
  }

  @Override
  public Class<String> getClazz() {
    return String.class;
  }

  @Override
  public DataPacker<String, char[]> getDataPacker() {
    return StringPacker.getInstance();
  }

  @Override
  public boolean isArray() {
    return false;
  }
}
