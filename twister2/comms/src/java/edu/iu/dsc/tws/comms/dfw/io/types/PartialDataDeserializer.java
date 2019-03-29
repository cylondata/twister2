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

import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class PartialDataDeserializer {
  private PartialDataDeserializer() {
  }

  public static int readFromBuffer(InMessage currentMessage, int currentLocation,
                                   DataBuffer buffer, int currentObjectLength,
                                   KryoSerializer serializer) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    switch (currentMessage.getDataType()) {
      case INTEGER:
        startIndex = startIndex / Integer.BYTES;
        int[] val = (int[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeInteger(buffer, currentObjectLength,
            val, startIndex, currentLocation);
      case LONG:
        startIndex = startIndex / Long.BYTES;
        long[] longVal = (long[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeLong(buffer, currentObjectLength,
            longVal, startIndex, currentLocation);
      case DOUBLE:
        startIndex = startIndex / Double.BYTES;
        double[] doubleVal = (double[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeDouble(buffer, currentObjectLength,
            doubleVal, startIndex, currentLocation);
      case SHORT:
        startIndex = startIndex / Short.BYTES;
        short[] shortVal = (short[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeShort(buffer, currentObjectLength,
            shortVal, startIndex, currentLocation);
      case BYTE:
        byte[] byteVal = (byte[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
            byteVal, startIndex, currentLocation);
      case OBJECT:
        byte[] objectVal = (byte[]) currentMessage.getDeserializingObject();
        int value = PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
            objectVal, startIndex, currentLocation);
        // at the end we switch to the actual object
        int totalBytesRead = startIndex + value;
        if (totalBytesRead == currentObjectLength) {
          Object kryoValue = serializer.deserialize(objectVal);
          currentMessage.setDeserializingObject(kryoValue);
        }
        return value;
      default:
        return 0;
    }
  }

  public static Object createDataObject(InMessage currentMessage, int length) {
    switch (currentMessage.getDataType()) {
      case INTEGER:
        return new int[length / Integer.BYTES];
      case LONG:
        return new long[length / Long.BYTES];
      case DOUBLE:
        return new double[length / Double.BYTES];
      case SHORT:
        return new short[length / Short.BYTES];
      case CHAR:
        return new char[length / Character.BYTES];
      case BYTE:
        return new byte[length];
      case OBJECT:
        return new byte[length];
      default:
        break;
    }
    return null;
  }

  public static int totalBytesRead(InMessage msg, int valsRead) {
    switch (msg.getDataType()) {
      case INTEGER:
        int i = valsRead + msg.getUnPkCurrentBytes() * Integer.BYTES;
        msg.addUnPkCurrentBytes(valsRead / Integer.BYTES);
        return i;
      case DOUBLE:
        int i1 = valsRead + msg.getUnPkCurrentBytes() * Double.BYTES;
        msg.addUnPkCurrentBytes(valsRead / Double.BYTES);
        return i1;
      case LONG:
        int i2 = valsRead + msg.getUnPkCurrentBytes() * Long.BYTES;
        msg.addUnPkCurrentBytes(valsRead / Long.BYTES);
        return i2;
      case SHORT:
        int i3 = valsRead + msg.getUnPkCurrentBytes() * Short.BYTES;
        msg.addUnPkCurrentBytes(valsRead / Short.BYTES);
        return i3;
      case CHAR:
        int i4 = valsRead + msg.getUnPkCurrentBytes() * Character.BYTES;
        msg.addUnPkCurrentBytes(valsRead / Character.BYTES);
        return i4;
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

  public static int deserializeInteger(DataBuffer buffers, int byteLength,
                                         int[] value, int startIndex, int bufferLocation) {
    int noOfInts = byteLength / Integer.BYTES;
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < noOfInts; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= Integer.BYTES) {
        value[i] = byteBuffer.getInt(currentBufferLocation);
        bytesRead += Integer.BYTES;
        currentBufferLocation += Integer.BYTES;
      } else {
        break;
      }
    }
    return bytesRead;
  }

  public static int deserializeLong(DataBuffer buffers, int byteLength,
                                       long[] value, int startIndex, int bufferLocation) {
    int noOfLongs = byteLength / Long.BYTES;
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < noOfLongs; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= Long.BYTES) {
        value[i] = byteBuffer.getLong(currentBufferLocation);
        bytesRead += Long.BYTES;
        currentBufferLocation += Long.BYTES;
      } else {
        break;
      }
    }
    return bytesRead;
  }

  public static int deserializeDouble(DataBuffer buffers, int byteLength,
                                    double[] value, int startIndex, int bufferLocation) {
    int noOfLongs = byteLength / Double.BYTES;
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < noOfLongs; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= Double.BYTES) {
        value[i] = byteBuffer.getDouble(currentBufferLocation);
        bytesRead += Double.BYTES;
        currentBufferLocation += Double.BYTES;
      } else {
        break;
      }
    }
    return bytesRead;
  }

  public static int deserializeShort(DataBuffer buffers, int byteLength,
                                      short[] value, int startIndex, int bufferLocation) {
    int noOfLongs = byteLength / Short.BYTES;
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < noOfLongs; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= Short.BYTES) {
        value[i] = byteBuffer.getShort(currentBufferLocation);
        bytesRead += Short.BYTES;
        currentBufferLocation += Short.BYTES;
      } else {
        break;
      }
    }
    return bytesRead;
  }

  public static int deserializeByte(DataBuffer buffers, int byteLength,
                                     byte[] value, int startIndex, int bufferLocation) {
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < byteLength; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= 1) {
        value[i] = byteBuffer.get(currentBufferLocation);
        bytesRead += 1;
        currentBufferLocation += 1;
      } else {
        break;
      }
    }
    return bytesRead;
  }
}
