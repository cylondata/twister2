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
package edu.iu.dsc.tws.comms.api.types;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.types.EmptyPacker;

public class EmptyType implements MessageType<Object, Object> {

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public int getUnitSizeInBytes() {
    return 0;
  }

  @Override
  public int getDataSizeInBytes(Object data) {
    return 0;
  }

  @Override
  public Class<Object> getClazz() {
    return Object.class;
  }

  @Override
  public DataPacker<Object, Object> getDataPacker() {
    return EmptyPacker.getInstance();
  }

  @Override
  public boolean isArray() {
    return false;
  }
}
