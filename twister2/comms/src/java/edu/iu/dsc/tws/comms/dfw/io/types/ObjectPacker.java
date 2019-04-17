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

import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.ObjectBuilder;
import edu.iu.dsc.tws.comms.api.PackerStore;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public final class ObjectPacker implements DataPacker<Object, byte[]> {

  private static volatile ObjectPacker instance;

  private KryoSerializer serializer;

  private ObjectPacker() {
    serializer = new KryoSerializer();
  }

  public static DataPacker<Object, byte[]> getInstance() {
    if (instance == null) {
      instance = new ObjectPacker();
    }
    return instance;
  }

  @Override
  public int determineLength(Object data, PackerStore store) {
    if (store.retrieve() == null) {
      byte[] serialize = serializer.serialize(data);
      store.store(serialize);
    }
    return store.retrieve().length;
  }

  @Override
  public void writeDataToBuffer(Object data, PackerStore packerStore,
                                int alreadyCopied, int leftToCopy, int spaceLeft,
                                ByteBuffer targetBuffer) {
    byte[] datBytes = packerStore.retrieve();
    targetBuffer.put(datBytes, alreadyCopied, Math.min(leftToCopy, spaceLeft));
  }

  @SuppressWarnings("unchecked")
  @Override
  public int readDataFromBuffer(ObjectBuilder objectBuilder,
                                int currentBufferLocation, DataBuffer dataBuffer) {
    int totalObjectLength = objectBuilder.getTotalSize();
    int startIndex = objectBuilder.getCompletedSize();
    byte[] objectVal = (byte[]) objectBuilder.getPartialDataHolder();
    int value = dataBuffer.copyPartToByteArray(currentBufferLocation, objectVal,
        startIndex, totalObjectLength);
    // at the end we switch to the actual object
    int totalBytesRead = startIndex + value;
    if (totalBytesRead == totalObjectLength) {
      Object kryoValue = serializer.deserialize(objectVal);
      objectBuilder.setFinalObject(kryoValue);
    }
    return value;
  }

  @Override
  public byte[] packToByteArray(Object data) {
    return this.serializer.serialize(data);
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, Object data) {
    return byteBuffer.put(this.packToByteArray(data));
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, Object data) {
    byte[] packedData = this.packToByteArray(data);
    for (int i = 0; i < packedData.length; i++) {
      byteBuffer.put(offset + i, packedData[i]);
    }
    return byteBuffer;
  }

  @Override
  public byte[] wrapperForByteLength(int byteLength) {
    return new byte[byteLength];
  }

  @Override
  public boolean isHeaderRequired() {
    return true;
  }

  @Override
  public Object unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    byte[] bytes = new byte[byteLength];
    // intentionally not using byteBuffer.get(byte[]). The contract of this method is not to update
    // buffer position
    for (int i = 0; i < byteLength; i++) {
      bytes[i] = byteBuffer.get(bufferOffset + i);
    }
    return this.serializer.deserialize(bytes);
  }

  @Override
  public Object unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    byte[] bytes = new byte[byteLength];
    byteBuffer.get(bytes, 0, byteLength);
    return null;
  }
}
