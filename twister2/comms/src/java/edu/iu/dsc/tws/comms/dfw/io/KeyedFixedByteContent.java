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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.comms.api.MessageType;

/**
 * Keyed content is serialized given priority and serialized as two parts of key and object.
 */
public class KeyedFixedByteContent {
  private int size;
  private final List<byte[]> source;

  private final List<byte[]> object;

  private MessageType keyType = MessageType.MULTI_FIXED_BYTE;

  private MessageType contentType = MessageType.MULTI_FIXED_BYTE;

  public KeyedFixedByteContent() {
    this.size = 0;
    this.source = new ArrayList<>();
    this.object = new ArrayList<>();
  }

  public KeyedFixedByteContent(List<byte[]> source, List<byte[]> object) {
    this.source = source;
    this.object = object;
    this.size = source.size();
  }

  public KeyedFixedByteContent(List<byte[]> source, List<byte[]> object,
                               MessageType keyType, MessageType contentType) {
    this.source = source;
    this.object = object;
    this.keyType = keyType;
    this.contentType = contentType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public Object getSource() {
    return source;
  }

  public Object getObject() {
    return object;
  }

  public MessageType getContentType() {
    return contentType;
  }

  public void add(byte[] key, byte[] value) {
    this.source.add(key);
    this.object.add(key);
    this.size += 1;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }
}
