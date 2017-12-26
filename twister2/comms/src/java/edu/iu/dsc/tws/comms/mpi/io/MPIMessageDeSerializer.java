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
package edu.iu.dsc.tws.comms.mpi.io;

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class MPIMessageDeSerializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMessageDeSerializer.class.getName());

  private KryoSerializer serializer;

  private MPIBuffer tempBuf;

  public MPIMessageDeSerializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg) {
    tempBuf = new MPIBuffer(1024);
  }

  @Override
  public Object build(Object partialObject, int edge) {
    MPIMessage currentMessage = (MPIMessage) partialObject;
    return buildMessage(currentMessage);
  }

  public MessageHeader buildHeader(MPIBuffer buffer, int edge) {
    int sourceId = buffer.getByteBuffer().getInt();
    int flags = buffer.getByteBuffer().getInt();
    int subEdge = buffer.getByteBuffer().getInt();
    int length = buffer.getByteBuffer().getInt();

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    // set the path
    headerBuilder.flags(flags);
    headerBuilder.destination(subEdge);

    // first build the header
    return headerBuilder.build();
  }

  private Object buildMessage(MPIMessage message) {
    MessageType type = message.getType();

    switch (type) {
      case INTEGER:
        break;
      case LONG:
        break;
      case DOUBLE:
        break;
      case OBJECT:
        return buildObject(message);
      case BYTE:
        break;
      case STRING:
        break;
      case BUFFER:
        return buildBuffer(message);
      default:
        break;
    }
    return null;
  }

  private Object buildBuffer(MPIMessage message) {
    return tempBuf;
  }

  private Object buildObject(MPIMessage message) {
    MPIByteArrayInputStream input = null;
    try {
      //todo: headersize
      input = new MPIByteArrayInputStream(message.getBuffers(), message.getHeaderSize(),
          message.getHeader().getLength());
      return serializer.deserialize(input);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException ignore) {
        }
      }
    }
  }
}
