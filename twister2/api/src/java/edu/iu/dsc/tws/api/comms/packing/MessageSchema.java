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

package edu.iu.dsc.tws.api.comms.packing;

/**
 * Define a message schema for a type
 */
public final class MessageSchema {

  private static final MessageSchema NO_SCHEMA = new MessageSchema();

  /**
   * Size of the message
   */
  private int messageSize; // total message size

  /**
   * Key size
   */
  private int keySize;

  /**
   * Weather we have fixed size messages
   */
  private boolean fixedSchema = true;

  private MessageSchema(int messageSize, int keySize) {
    this.messageSize = messageSize;
    this.keySize = keySize;
  }

  private MessageSchema(int messageSize) {
    this.messageSize = messageSize;
  }

  private MessageSchema() {
    this.fixedSchema = false;
  }

  public boolean isFixedSchema() {
    return fixedSchema;
  }

  public int getMessageSize() {
    return messageSize;
  }

  public int getKeySize() {
    return keySize;
  }

  public static MessageSchema noSchema() {
    return NO_SCHEMA;
  }

  public static MessageSchema ofSize(int totalMessageSize) {
    return new MessageSchema(totalMessageSize);
  }

  public static MessageSchema ofSize(int totalMessageSize, int keySize) {
    return new MessageSchema(totalMessageSize, keySize);
  }
}
