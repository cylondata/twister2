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
package edu.iu.dsc.tws.common.net.tcp.request;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class MessageID {
  public static final int MESSAGE_ID_SIZE = 32;
  private static Random randomGenerator = new Random(System.nanoTime());
  private byte[] bytes;

  public MessageID(byte[] dataBytes) {
    assert dataBytes.length == MESSAGE_ID_SIZE;
    bytes = new byte[MESSAGE_ID_SIZE];
    System.arraycopy(dataBytes, 0, bytes, 0, dataBytes.length);
  }

  public MessageID(ByteBuffer buffer) {
    bytes = new byte[MessageID.MESSAGE_ID_SIZE];
    buffer.get(bytes);
  }

  public static MessageID create() {
    byte[] dataBytes = new byte[MESSAGE_ID_SIZE];
    randomGenerator.nextBytes(dataBytes);
    return new MessageID(dataBytes);
  }

  public byte[] value() {
    return bytes;
  }

  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other == this) {
      return true;
    }
    if (!(other instanceof MessageID)) {
      return false;
    }
    MessageID rother = (MessageID) other;
    return Arrays.equals(this.bytes, rother.value());
  }

  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  @Override
  public String toString() {
    StringBuilder bldr = new StringBuilder();
    for (int j = 0; j < bytes.length; j++) {
      bldr.append(String.format("%02X ", bytes[j]));
    }
    return bldr.toString();
  }
}
