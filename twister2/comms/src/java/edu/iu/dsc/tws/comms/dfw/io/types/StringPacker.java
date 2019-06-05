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

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.ObjectBuilder;
import edu.iu.dsc.tws.comms.api.PackerStore;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.CharArrayPacker;

public final class StringPacker implements DataPacker<String, char[]> {

  private static final String KEY_CHARS = "CHARS";
  private static final CharArrayPacker CHAR_ARRAY_PACKER = CharArrayPacker.getInstance();

  private static volatile StringPacker instance;

  private StringPacker() {
  }

  public static StringPacker getInstance() {
    if (instance == null) {
      instance = new StringPacker();
    }
    return instance;
  }

  @Override
  public int determineLength(String data, PackerStore store) {
    char[] chars = data.toCharArray();
    //since toCharArray is expensive, saving it to state
    store.put(KEY_CHARS, chars);
    return chars.length * Character.BYTES;
  }

  @Override
  public void writeDataToBuffer(String data, PackerStore packerStore,
                                int alreadyCopied, int leftToCopy, int spaceLeft,
                                ByteBuffer targetBuffer) {
    CHAR_ARRAY_PACKER.writeDataToBuffer(
        (char[]) packerStore.get(KEY_CHARS),
        packerStore,
        alreadyCopied,
        leftToCopy,
        spaceLeft,
        targetBuffer
    );
  }

  @Override
  public int readDataFromBuffer(ObjectBuilder<String, char[]> objectBuilder,
                                int currentBufferLocation, DataBuffer dataBuffer) {
    int totalDataLength = objectBuilder.getTotalSize();
    int startIndex = objectBuilder.getCompletedSize();
    int unitSize = Character.BYTES;
    startIndex = startIndex / unitSize;
    char[] val = objectBuilder.getPartialDataHolder();

    //deserializing
    int noOfElements = totalDataLength / unitSize;
    int bufferPosition = currentBufferLocation;
    int bytesRead = 0;
    for (int i = startIndex; i < noOfElements; i++) {
      ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
      int remaining = dataBuffer.getSize() - bufferPosition;
      if (remaining >= unitSize) {
        val[i] = byteBuffer.getChar(bufferPosition);
        bytesRead += unitSize;
        bufferPosition += unitSize;
      } else {
        break;
      }
    }
    if (totalDataLength == bytesRead + startIndex) {
      objectBuilder.setFinalObject(new String(val));
    }
    return bytesRead;
  }

  @Override
  public byte[] packToByteArray(String data) {
    return CHAR_ARRAY_PACKER.packToByteArray(data.toCharArray());
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, String data) {
    for (int i = 0; i < data.length(); i++) {
      byteBuffer.putChar(data.charAt(i));
    }
    return byteBuffer;
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, String data) {
    for (int i = 0; i < data.length(); i++) {
      byteBuffer.putChar(offset + i, data.charAt(i));
    }
    return byteBuffer;
  }

  @Override
  public char[] wrapperForByteLength(int byteLength) {
    return CHAR_ARRAY_PACKER.wrapperForByteLength(byteLength);
  }

  @Override
  public boolean isHeaderRequired() {
    return true;
  }

  @Override
  public String unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    char[] chars = CHAR_ARRAY_PACKER.unpackFromBuffer(byteBuffer, bufferOffset, byteLength);
    return String.valueOf(chars);
  }

  @Override
  public String unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    char[] chars = CHAR_ARRAY_PACKER.unpackFromBuffer(byteBuffer, byteLength);
    return String.valueOf(chars);
  }
}
