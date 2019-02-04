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
package edu.iu.dsc.tws.comms.dfw.io.types;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public final class PartialKeyDeSerializer {
  private PartialKeyDeSerializer() {
  }

  public static Pair<Integer, Integer> createKey(InMessage message,
                                                 DataBuffer buffers) {
    int keyLength = 0;
    int readBytes = 0;
    // first we need to read the key type
    switch (message.getKeyType()) {
      case INTEGER:
        keyLength = Integer.BYTES;
        break;
      case SHORT:
        keyLength = Short.BYTES;
        break;
      case LONG:
        keyLength = Long.BYTES;
        break;
      case DOUBLE:
        keyLength = Double.BYTES;
        break;
      case OBJECT:
        keyLength = buffers.getByteBuffer().getInt();
        readBytes = Integer.BYTES;
        byte[] b = new byte[keyLength];
        message.setDeserializingKey(b);
        break;
      case BYTE:
        keyLength = buffers.getByteBuffer().getInt();
        readBytes = Integer.BYTES;
        byte[] bytes = new byte[keyLength];
        message.setDeserializingKey(bytes);
        break;
      default:
        break;
    }

    return new ImmutablePair<>(keyLength, readBytes);
  }
}
