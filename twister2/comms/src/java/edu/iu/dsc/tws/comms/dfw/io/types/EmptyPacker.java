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

public class EmptyPacker implements DataPacker<Object, Object> {

  private static final Object EMPTY_OBJECT = new Object();

  private static volatile EmptyPacker instance;

  public static EmptyPacker getInstance() {
    if (instance == null) {
      instance = new EmptyPacker();
    }
    return instance;
  }

  @Override
  public int determineLength(Object data, PackerStore store) {
    return 0;
  }

  @Override
  public void writeDataToBuffer(Object data, PackerStore packerStore,
                                int alreadyCopied, int leftToCopy, int spaceLeft,
                                ByteBuffer targetBuffer) {
    //do nothing
  }

  @Override
  public int readDataFromBuffer(ObjectBuilder<Object, Object> objectBuilder,
                                int currentBufferLocation, DataBuffer dataBuffer) {
    return 0;
  }

  @Override
  public byte[] packToByteArray(Object data) {
    return new byte[0];
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, Object data) {
    return byteBuffer;
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, Object data) {
    return byteBuffer;
  }

  @Override
  public Object wrapperForByteLength(int byteLength) {
    return EMPTY_OBJECT;
  }

  @Override
  public boolean isHeaderRequired() {
    return false;
  }

  @Override
  public Object unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    return EMPTY_OBJECT;
  }

  @Override
  public Object unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    return EMPTY_OBJECT;
  }
}
