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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeyDeserializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.MessageTypeUtils;

/**
 * Deserialize a single message in a buffer
 */
public class SingleMessageDeSerializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(SingleMessageDeSerializer.class.getName());

  /**
   * The kryo serializer
   */
  private KryoSerializer serializer;

  /**
   * Weather keys are used
   */
  private boolean keyed;

  public SingleMessageDeSerializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg, boolean k) {
    this.keyed = k;
  }

  @Override
  public Object build(Object partialObject, int edge) {
    ChannelMessage currentMessage = (ChannelMessage) partialObject;
    return buildMessage(currentMessage);
  }

  public MessageHeader buildHeader(DataBuffer buffer, int edge) {
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

  @Override
  @SuppressWarnings("unchecked")
  public Object getDataBuffers(Object partialObject, int edge) {
    ChannelMessage message = (ChannelMessage) partialObject;
    MessageType type = message.getType();
    //Used when handling multi messages
    List<ImmutablePair<byte[], byte[]>> results;
    if (!keyed) {
      return DataDeserializer.getAsByteBuffer(message.getBuffers(),
          message.getHeader().getLength(), type);
    } else {

      Pair<Integer, Object> keyPair = KeyDeserializer.
          getKeyAsByteBuffer(message.getKeyType(),
              message.getBuffers());
      MessageType keyType = message.getKeyType();
      Object data;

      if (MessageTypeUtils.isMultiMessageType(keyType)) {
        data = DataDeserializer.getAsByteBuffer(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - 4 - 4, type,
            ((List) keyPair.getValue()).size());
        results = new ArrayList<>();
        List<byte[]> keyList = (List<byte[]>) keyPair.getValue();
        List<byte[]> dataList = (List<byte[]>) data;
        for (int i = 0; i < keyList.size(); i++) {
          results.add(new ImmutablePair<>(keyList.get(i), dataList.get(i)));
        }
        return results;
      } else if (!MessageTypeUtils.isPrimitiveType(keyType)) {
        data = DataDeserializer.getAsByteBuffer(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - 4, type);
      } else {
        data = DataDeserializer.getAsByteBuffer(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey(), type);
      }

      return new ImmutablePair<>(keyPair.getValue(), data);
    }
  }

  @SuppressWarnings("unchecked")
  private Object buildMessage(ChannelMessage message) {
    MessageType type = message.getType();

    if (!keyed) {
      return DataDeserializer.deserializeData(message.getBuffers(),
          message.getHeader().getLength(), serializer, type);
    } else {
      Pair<Integer, Object> keyPair = KeyDeserializer.deserializeKey(message.getKeyType(),
          message.getBuffers(), serializer);
      MessageType keyType = message.getKeyType();
      Object data;
      List<ImmutablePair<byte[], byte[]>> results;

      if (MessageTypeUtils.isMultiMessageType(keyType)) {
        List<byte[]> keyList = (List<byte[]>) keyPair.getValue();
        data = DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - 4 - 4, serializer, type,
            ((List) keyPair.getValue()).size());
        List<byte[]> dataList = (List<byte[]>) data;
        results = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
          results.add(new ImmutablePair<>(keyList.get(i), dataList.get(i)));
        }
        return results;
      } else if (!MessageTypeUtils.isPrimitiveType(keyType)) {
        return DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - 4, serializer, type);
      } else {
        return DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey(), serializer, type);
      }
    }
  }
}
