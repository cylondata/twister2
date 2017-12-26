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

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;
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
   * @param buffer
   */
  public void serializeObject(Object object, MPISendMessage sendMessage, MPIBuffer buffer) {
    byte[] data;
    int dataPosition;
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    if (sendMessage.serializedState() == MPISendMessage.SendState.HEADER_BUILT) {
      // okay we need to serialize the data
      data = serializer.serialize(object);
      // at this point we know the length of the data
      byteBuffer.putInt(12, data.length);
      // now lets set the header
      MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
          sendMessage.getEdge(), data.length);
      builder.destination(sendMessage.getDestintationIdentifier());
      sendMessage.getMPIMessage().setHeader(builder.build());
      dataPosition = 0;
      sendMessage.setSerializationState(data);
//      LOG.log(Level.INFO, String.format("Finished adding header %d %d %d %d",
//          sendMessage.getSource(), sendMessage.getEdge(), sendMessage.getPath(), data.length));
    } else {
      data = (byte[]) sendMessage.getSerializationState();
      dataPosition = sendMessage.getByteCopied();
    }

    int remainingToCopy = data.length - dataPosition;
    // check how much space we have
    int bufferSpace = byteBuffer.capacity() - byteBuffer.position();

    int copyBytes = remainingToCopy > bufferSpace ? bufferSpace : remainingToCopy;
    // check how much space left in the buffer
    byteBuffer.put(data, dataPosition, copyBytes);
    sendMessage.setByteCopied(dataPosition + copyBytes);

    // now set the size of the buffer
//    LOG.log(Level.INFO, String.format("Serialize object body with buffer size: %d copyBytes: "
//        + "%d remainingCopy: %d", byteBuffer.position(), copyBytes, remainingToCopy));
    buffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (copyBytes == remainingToCopy) {
      sendMessage.setSendState(MPISendMessage.SendState.SERIALIZED);
    } else {
      sendMessage.setSendState(MPISendMessage.SendState.BODY_BUILT);
    }
  }
}
