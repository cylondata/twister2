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

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class PartialKeyDeSerializer {
  private PartialKeyDeSerializer() {
  }

  public static int keyLengthHeaderSize(InMessage message) {
    if (message.getKeyType() == MessageType.BYTE || message.getKeyType() == MessageType.OBJECT) {
      return Integer.BYTES;
    } else {
      return 0;
    }
  }

  public static Pair<Integer, Integer> createKey(InMessage message,
                                                 DataBuffer buffers, int location) {
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
        keyLength = buffers.getByteBuffer().getInt(location);
        readBytes = Integer.BYTES;
        break;
      case BYTE:
        keyLength = buffers.getByteBuffer().getInt(location);
        readBytes = Integer.BYTES;
        break;
      default:
        break;
    }

    return new ImmutablePair<>(keyLength, readBytes);
  }

  public static Object createKeyObject(MessageType messageType, int length) {
    switch (messageType) {
      case INTEGER:
        return null;
      case LONG:
        return null;
      case DOUBLE:
        return null;
      case SHORT:
        return null;
      case CHAR:
        return null;
      case BYTE:
        return new byte[length];
      case OBJECT:
        return new byte[length];
      default:
        break;
    }
    return null;
  }

  public static int readFromBuffer(InMessage currentMessage, int currentLocation,
                                   DataBuffer buffer, int currentObjectLength,
                                   KryoSerializer serializer) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    switch (currentMessage.getDataType()) {
      case INTEGER:
        return deserializeInteger(currentMessage, buffer, currentLocation);
      case LONG:
        return deserializeLong(currentMessage, buffer, currentLocation);
      case DOUBLE:
        return deserializeDouble(currentMessage, buffer, currentLocation);
      case SHORT:
        return deserializeShort(currentMessage, buffer, currentLocation);
      case BYTE:
        byte[] byteVal = (byte[]) currentMessage.getDeserializingKey();
        return PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
            byteVal, startIndex, currentLocation);
      case OBJECT:
        byte[] objectVal = (byte[]) currentMessage.getDeserializingKey();
        int value = PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
            objectVal, startIndex, currentLocation);
        // at the end we switch to the actual object
        if (value == currentObjectLength) {
          Object kryoValue = serializer.deserialize(objectVal);
          currentMessage.setDeserializingObject(kryoValue);
        }
        return value;
      default:
        return 0;
    }
  }

  public static int totalBytesRead(InMessage msg, int valsRead) {
    switch (msg.getDataType()) {
      case INTEGER:
        return valsRead;
      case DOUBLE:
        return valsRead;
      case LONG:
        return valsRead;
      case SHORT:
        return valsRead;
      case CHAR:
        return valsRead;
      case BYTE:
        int i5 = valsRead + msg.getUnPkCurrentBytes();
        msg.addUnPkCurrentBytes(valsRead);
        return i5;
      case OBJECT:
        int i6 = valsRead + msg.getUnPkCurrentBytes();
        msg.addUnPkCurrentBytes(valsRead);
        return i6;
      default:
        break;
    }
    return 0;
  }

  private static int deserializeInteger(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Integer.BYTES) {
      int val = byteBuffer.getInt(bufferLocation);
      message.setDeserializingKey(val);
      return Integer.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeLong(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Long.BYTES) {
      long val = byteBuffer.getLong(bufferLocation);
      message.setDeserializingKey(val);
      return Long.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeDouble(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Double.BYTES) {
      double val = byteBuffer.getDouble(bufferLocation);
      message.setDeserializingKey(val);
      return Double.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeShort(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Short.BYTES) {
      short val = byteBuffer.getShort(bufferLocation);
      message.setDeserializingKey(val);
      return Short.BYTES;
    } else {
      return 0;
    }
  }
}
