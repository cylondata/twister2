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
import java.util.UUID;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.KeyPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.types.ObjectPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.BytePacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.DoublePacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.FloatPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.LongPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.ShortPacker;

public final class DFWIOUtils {
  private static final int HEADER_SIZE = 16;

  private DFWIOUtils() {
    throw new UnsupportedOperationException();
  }

  public static String getOperationName(int target, DataFlowOperation op) {
    String uid = op.getUniqueId();
    return "partition-" + uid + "-" + target + "-" + UUID.randomUUID().toString();
  }

  /**
   * Builds the header of the message. The length value is inserted later so 0 is added as a place
   * holder value. The header structure is |source|flags|destinationID|length|
   *
   * @param buffer the buffer to which the header is placed
   * @param sendMessage the message that the header is build for
   */
  public static void buildHeader(DataBuffer buffer, OutMessage sendMessage) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    // the destination id
    byteBuffer.putInt(sendMessage.getPath());
    // we set the number of messages
    byteBuffer.putInt(0);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
  }

  /**
   * Builds the header of the message. The length value is inserted later so 0 is added as a place
   * holder value. The header structure is |source|flags|destinationID|length|
   *
   * @param buffer the buffer to which the header is placed
   * @param sendMessage the message that the header is build for
   * @param noOfMessages number of messages in this single message
   */
  public static void buildHeader(DataBuffer buffer, OutMessage sendMessage, int noOfMessages) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    // the destination id
    byteBuffer.putInt(sendMessage.getPath());
    // we set the number of messages
    byteBuffer.putInt(noOfMessages);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
  }

  /**
   * This method will not be necessary. Use {@link MessageType#getDataPacker()} instead
   *
   * @return Data packer
   * @deprecated Use {@link MessageType#getDataPacker()} instead
   */
  @Deprecated
  public static DataPacker createPacker(MessageType messageType) {
    if (messageType == MessageType.CUSTOM) {
      return messageType.getDataPacker();
    }

    if (messageType == MessageType.INTEGER) {
      return messageType.getDataPacker();
    } else if (messageType == MessageType.LONG) {
      return messageType.getDataPacker();
    } else if (messageType == MessageType.SHORT) {
      return messageType.getDataPacker();
    } else if (messageType == MessageType.DOUBLE) {
      return messageType.getDataPacker();
    } else if (messageType == MessageType.BYTE) {
      return messageType.getDataPacker();
    } else if (messageType == MessageType.OBJECT) {
      return new ObjectPacker();
    } else {
      return messageType.getDataPacker();
    }
  }

  /**
   * This method will not be necessary. Use {@link MessageType#getDataPacker()} instead
   *
   * @return Key packer
   * @deprecated Use {@link MessageType#getDataPacker()} instead
   */
  @Deprecated
  public static KeyPacker createKeyPacker(MessageType dataType) {
    if (dataType == MessageType.CUSTOM) {
      return dataType.getKeyPacker();
    }

    if (dataType == MessageType.INTEGER) {
      return IntegerPacker.getInstance();
    } else if (dataType == MessageType.LONG) {
      return LongPacker.getInstance();
    } else if (dataType == MessageType.SHORT) {
      return ShortPacker.getInstance();
    } else if (dataType == MessageType.DOUBLE) {
      return DoublePacker.getInstance();
    } else if (dataType == MessageType.FLOAT) {
      return FloatPacker.getInstance();
    } else if (dataType == MessageType.BYTE) {
      return BytePacker.getInstance();
    }
    return null;
  }
}


