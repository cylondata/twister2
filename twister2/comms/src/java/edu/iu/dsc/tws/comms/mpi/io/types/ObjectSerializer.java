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
package edu.iu.dsc.tws.comms.mpi.io.types;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;
import edu.iu.dsc.tws.comms.mpi.io.SerializeState;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class ObjectSerializer {
  private KryoSerializer serializer;

  public ObjectSerializer(KryoSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Serializes a java object using kryo serialization
   *
   * @param object
   * @param sendMessage
   * @param targetBuffer
   */
  public boolean serializeObject(Object object,
                                 MPISendMessage sendMessage, MPIBuffer targetBuffer) {
    byte[] data;
    int dataPosition = 0;
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    SerializeState state = sendMessage.getSerializationState();
    int totalBytes = state.getTotalBytes();
    if (state.getBytesCopied() == 0) {
      // okay we need to serialize the data
      data = serializer.serialize(object);
      state.setData(data);
//      LOG.log(Level.INFO, String.format("Finished adding header %d %d %d %d",
//          sendMessage.getSource(), sendMessage.getEdge(), sendMessage.getPath(), data.length));
    } else {
      data = sendMessage.getSerializationState().getData();
      dataPosition = state.getBytesCopied();
    }

    int remainingToCopy = data.length - dataPosition;
    // check how much space we have
    int bufferSpace = byteBuffer.capacity() - byteBuffer.position();

    int copyBytes = remainingToCopy > bufferSpace ? bufferSpace : remainingToCopy;
    // check how much space left in the buffer
    byteBuffer.put(data, dataPosition, copyBytes);
    state.setBytesCopied(dataPosition + copyBytes);
    state.setTotalBytes(totalBytes + copyBytes);
//    LOG.log(Level.INFO, String.format("Serialize object body with buffer size: %d copyBytes: "
//        + "%d remainingCopy: %d", byteBuffer.position(), copyBytes, remainingToCopy));
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());
    // okay we are done with the message
    // okay we are done with the message
    if (copyBytes == remainingToCopy) {
      state.setBytesCopied(0);
      state.setBufferNo(0);
      state.setData(null);
      return true;
    } else {
      return false;
    }
  }
}
