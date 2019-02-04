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

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class PartialDataDeserializer {
  private PartialDataDeserializer() {
  }

  public static int readFromBuffer(InMessage currentMessage, int currentLocation,
                                   DataBuffer buffer, int currentObjectLength,
                                   KryoSerializer serializer) {
    int startIndex = currentMessage.getUnPkCurrentIndex();
    switch (currentMessage.getDataType()) {
      case INTEGER:
        int[] val = (int[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeInteger(buffer, currentObjectLength,
            val, startIndex, currentLocation);
      case LONG:
        long[] longVal = (long[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeLong(buffer, currentObjectLength,
            longVal, startIndex, currentLocation);
      case DOUBLE:
        double[] doubleVal = (double[]) currentMessage.getDeserializingObject();
        return PartialDataDeserializer.deserializeDouble(buffer, currentObjectLength,
            doubleVal, startIndex, currentLocation);
      case SHORT:
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
        if (value == currentObjectLength) {
          Object kryoValue = serializer.deserialize(objectVal);
          currentMessage.setDeserializingObject(kryoValue);
        }
        return value;
      default:
        return 0;
    }
  }

  public static void createDataObject(InMessage currentMessage, int currentLocation) {
    switch (currentMessage.getDataType()) {
      case INTEGER:
        int[] value = new int[currentLocation];
        currentMessage.setDeserializingObject(value);
        break;
      case LONG:
        long[] longValue = new long[currentLocation];
        currentMessage.setDeserializingObject(longValue);
        break;
      case DOUBLE:
        double[] doubleValue = new double[currentLocation];
        currentMessage.setDeserializingObject(doubleValue);
        break;
      case SHORT:
        short[] shortValue = new short[currentLocation];
        currentMessage.setDeserializingObject(shortValue);
        break;
      case CHAR:
        char[] charValue = new char[currentLocation];
        currentMessage.setDeserializingObject(charValue);
        break;
      case BYTE:
        byte[] byteValue = new byte[currentLocation];
        currentMessage.setDeserializingObject(byteValue);
        break;
      case OBJECT:
        byte[] objectValue = new byte[currentLocation];
        currentMessage.setDeserializingObject(objectValue);
        break;
      default:
        break;
    }
    currentMessage.setUnPkCurrentIndex(0);
  }

  public static int totalBytesRead(MessageType type, int index, int valsRead) {
    switch (type) {
      case INTEGER:
        return valsRead + index * Integer.BYTES;
      case DOUBLE:
        return valsRead + index * Double.BYTES;
      case LONG:
        return valsRead + index * Long.BYTES;
      case SHORT:
        return valsRead + index * Short.BYTES;
      case CHAR:
        return valsRead + index * Character.BYTES;
      case BYTE:
        return valsRead + index;
      case OBJECT:
        return valsRead + index;
      default:
        break;
    }
    return 0;
  }

  private static int deserializeInteger(DataBuffer buffers, int byteLength,
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

  private static int deserializeLong(DataBuffer buffers, int byteLength,
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

  private static int deserializeDouble(DataBuffer buffers, int byteLength,
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

  private static int deserializeShort(DataBuffer buffers, int byteLength,
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

  private static int deserializeByte(DataBuffer buffers, int byteLength,
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
