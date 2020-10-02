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
package edu.iu.dsc.tws.comms.dfw.io;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;

public final class DataPackerProxy {

  private DataPackerProxy() {
  }

  static boolean writeDataToBuffer(DataPacker dataPacker,
                                   Object data,
                                   ByteBuffer byteBuffer,
                                   SerializeState state) {

    SerializeState.StoredData activeStoredData = state.getActive();

    int spaceLeft = byteBuffer.remaining();

    //this much of bytes are left to copy in current object
    int leftToCopy = activeStoredData.leftToCopy();
    int alreadyCopied = activeStoredData.getBytesCopied();

    dataPacker.writeDataToBuffer(data, state, alreadyCopied, leftToCopy, spaceLeft, byteBuffer);

    int spaceLeftAfterCopying = byteBuffer.remaining();

    activeStoredData.incrementCopied(spaceLeft - spaceLeftAfterCopying);
    state.incrementTotalBytes(spaceLeft - spaceLeftAfterCopying);

    if (activeStoredData.hasCompleted()) {
      state.clearActive();
      return true;
    }
    return false;
  }

  static int getKeyLengthLeft(MessageType typeDefinition,
                              DataBuffer buffer, int location) {
    if (!typeDefinition.getDataPacker().isHeaderRequired()) {
      return typeDefinition.getUnitSizeInBytes();
    } else {
      return buffer.getByteBuffer().getInt(location);
    }
  }

  static int getKeyLengthRight(MessageType typeDefinition,
                               DataBuffer buffer, int location) {
    if (!typeDefinition.getDataPacker().isHeaderRequired()) {
      return 0;
    } else {
      return Integer.BYTES;
    }
  }
}
