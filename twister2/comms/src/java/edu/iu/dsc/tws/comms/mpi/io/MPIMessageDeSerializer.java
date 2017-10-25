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

public class MPIMessageDeSerializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMessageDeSerializer.class.getName());

  private KryoSerializer serializer;

  private boolean grouped;

  public MPIMessageDeSerializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg, boolean grped) {
    this.grouped = grped;
  }

  @Override
  public Object buid(Object message, Object partialObject, int edge) {
    MPIMessage currentMessage = (MPIMessage) partialObject;
    MPIBuffer buffer = (MPIBuffer) message;

    if (currentMessage.getHeader() == null) {
      buildHeader(buffer, currentMessage, edge);
    }

    if (!currentMessage.isComplete()) {
      currentMessage.addBuffer(buffer);
      currentMessage.build();
    }

    // we received a message, we need to determine weather we need to
    // forward to another node and process
    if (currentMessage.isComplete()) {
      return buildMessage(currentMessage);
    }

    return null;
  }

  private void buildHeader(MPIBuffer buffer, MPIMessage message, int edge) {
    int sourceId = buffer.getByteBuffer().getInt();
    int path = buffer.getByteBuffer().getInt();
    int subEdge = buffer.getByteBuffer().getInt();
    int length = buffer.getByteBuffer().getInt();

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    // set the path
    headerBuilder.path(path);
    headerBuilder.subEdge(subEdge);

    // first build the header
    message.setHeader(headerBuilder.build());
    LOG.info(String.format("Received message header: %d %d %d %d",
        sourceId, path, subEdge, length));
    // we set the 20 header size for now
    message.setHeaderSize(16);
  }

  protected Object buildMessage(MPIMessage message) {
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
      default:
        break;
    }
    return null;
  }

  private Object buildObject(MPIMessage message) {
    MPIByteArrayInputStream input = null;
    try {
      //todo: headersize
      input = new MPIByteArrayInputStream(message.getBuffers(), message.getHeaderSize(), grouped);
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
