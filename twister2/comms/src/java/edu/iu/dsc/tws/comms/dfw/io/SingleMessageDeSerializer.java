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
   * The kryo deserializer
   */
  private KryoSerializer deserializer;

  /**
   * Weather keys are used
   */
  private boolean keyed;

  /**
   * The length of the filed that keeps the key length for non primitive keys
   * ex. length of an key of type object
   */
  private static final int KEY_LENGTH_FEILD_SIZE = 4;

  /**
   * The length of the filed that keeps the key length in multi messages
   */
  private static final int MULTI_MESSAGE_KEY_LENGTH_FEILD_SIZE = 8;


  public SingleMessageDeSerializer(KryoSerializer kryoDeSerializer) {
    this.deserializer = kryoDeSerializer;
  }

  @Override
  public void init(Config cfg, boolean k) {
    this.keyed = k;
  }

  /**
   * Builds the message from the data buffers in the partialObject
   *
   * @param partialObject message object that needs to be built
   * @param edge the edge value associated with this message
   * @return the built message as a object
   */
  @Override
  public Object build(Object partialObject, int edge) {
    ChannelMessage currentMessage = (ChannelMessage) partialObject;
    return buildMessage(currentMessage);
  }

  /**
   * Builds the header object from the data in the data buffer
   *
   * @param buffer data buffer that contains the message
   * @param edge the edge value associated with this message
   * @return the built message header object
   */
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

  /**
   * Gets the message data in the message buffers as a byte[]
   *
   * @param partialObject object that contains the buffers
   * @param edge the edge value associated with this message
   * @return the message as a byte[]
   */
  @Override
  @SuppressWarnings("unchecked")
  public Object getDataBuffers(Object partialObject, int edge) {
    ChannelMessage message = (ChannelMessage) partialObject;
    MessageType type = message.getType();
    //Used when handling multi messages
    List<KeyedContent> results;
    if (!keyed) {
      return DataDeserializer.getAsByteArray(message.getBuffers(),
          message.getHeader().getLength(), type);
    } else {

      Pair<Integer, Object> keyPair = KeyDeserializer.
          getKeyAsByteArray(message.getKeyType(),
              message.getBuffers());
      MessageType keyType = message.getKeyType();
      Object data;

      if (MessageTypeUtils.isMultiMessageType(keyType)) {
        data = DataDeserializer.getAsByteArray(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey()
                - MULTI_MESSAGE_KEY_LENGTH_FEILD_SIZE, type, ((List) keyPair.getValue()).size());
        results = new ArrayList<>();
        List<byte[]> keyList = (List<byte[]>) keyPair.getValue();
        List<byte[]> dataList = (List<byte[]>) data;
        for (int i = 0; i < keyList.size(); i++) {
          results.add(new KeyedContent(keyList.get(i), dataList.get(i),
              message.getKeyType(), type));
        }
        return results;
      } else if (!MessageTypeUtils.isPrimitiveType(keyType)) {
        data = DataDeserializer.getAsByteArray(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - KEY_LENGTH_FEILD_SIZE, type);
      } else {
        data = DataDeserializer.getAsByteArray(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey(), type);
      }
      return new KeyedContent(keyPair.getValue(), data,
          message.getKeyType(), type);
    }
  }

  /**
   * Builds the message from the data in the data buffers.
   *
   * @param message the object that contains all the message details and data buffers
   * @return the built message object
   */
  @SuppressWarnings("unchecked")
  private Object buildMessage(ChannelMessage message) {
    MessageType type = message.getType();

    if (!keyed) {
      return DataDeserializer.deserializeData(message.getBuffers(),
          message.getHeader().getLength(), deserializer, type);
    } else {
      Pair<Integer, Object> keyPair = KeyDeserializer.deserializeKey(message.getKeyType(),
          message.getBuffers(), deserializer);
      MessageType keyType = message.getKeyType();
      Object data;
      List<KeyedContent> results;

      if (MessageTypeUtils.isMultiMessageType(keyType)) {
        List<byte[]> keyList = (List<byte[]>) keyPair.getValue();
        data = DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey()
                - MULTI_MESSAGE_KEY_LENGTH_FEILD_SIZE, deserializer, type,
            ((List) keyPair.getValue()).size());
        List<byte[]> dataList = (List<byte[]>) data;
        results = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
          results.add(new KeyedContent(keyList.get(i), dataList.get(i),
              message.getKeyType(), type));
        }
        return results;
      } else if (!MessageTypeUtils.isPrimitiveType(keyType)) {
        Object d = DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey() - KEY_LENGTH_FEILD_SIZE,
            deserializer, type);
        return new KeyedContent(keyPair.getValue(), d, message.getKeyType(), type);
      } else {
        Object d = DataDeserializer.deserializeData(message.getBuffers(),
            message.getHeader().getLength() - keyPair.getKey(), deserializer, type);
        return new KeyedContent(keyPair.getValue(), d, message.getKeyType(), type);
      }
    }
  }
}
