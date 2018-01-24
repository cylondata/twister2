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
package edu.iu.dsc.tws.comms.mpi.io;

import edu.iu.dsc.tws.comms.api.MessageType;

/**
 * Keyed content is serialized given priority and serialized as two parts of key and object.
 */
public class KeyedContent {
  private final Object source;

  private final Object object;

  private MessageType keyType = MessageType.SHORT;

  private MessageType contentType = MessageType.OBJECT;

  public KeyedContent(Object source, Object object) {
    this.source = source;
    this.object = object;
  }

  public KeyedContent(Object source, Object object,
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
}
