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
import java.util.Map;
import java.util.UUID;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.OutMessage;

public final class DFWIOUtils {

  // source(int)+flags(int)+path(int)+num_of_msgs(int)+first_buffer_flag(short)
  public static final int HEADER_SIZE = 17; // header size of first buffer

  // source(int)+first_buffer_flag(short)
  public static final int SHORT_HEADER_SIZE = 5; // header size of buffers other than the first one

  private DFWIOUtils() {
    throw new UnsupportedOperationException();
  }

  public static String getOperationName(int target, DataFlowOperation op, int refresh) {
    String uid = op.getUniqueId();
    return "partition-" + uid + "-" + target + "-" + UUID.randomUUID().toString() + "-" + refresh;
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
    // mark this buffer as "not the last buffer" of this message
    byteBuffer.put((byte) 0);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
  }

  public static boolean sendSyncForward(boolean needsFurtherProgress, int target,
                                        SyncState syncState, Map<Integer, byte[]> barriers,
                                        DataFlowOperation operation,
                                        Map<Integer, Boolean> isSyncSent) {
    byte[] message;
    int flags;
    if (syncState == SyncState.SYNC) {
      flags = MessageFlags.SYNC_EMPTY;
      message = new byte[1];
    } else {
      flags = MessageFlags.SYNC_BARRIER;
      message = barriers.get(target);
    }

    if (operation.sendPartial(target, message, flags)) {
      isSyncSent.put(target, true);
    } else {
      return true;
    }
    return needsFurtherProgress;
  }

  public static boolean sendFinalSyncForward(boolean needsFurtherProgress, int target,
                                             SyncState syncState, Map<Integer, byte[]> barriers,
                                             DataFlowOperation operation,
                                             Map<Integer, Boolean> isSyncSent) {
    byte[] message;
    int flags;
    if (syncState == SyncState.SYNC) {
      flags = MessageFlags.SYNC_EMPTY;
      message = new byte[1];
    } else {
      flags = MessageFlags.SYNC_BARRIER;
      message = barriers.get(target);
    }

    if (operation.send(target, message, flags)) {
      isSyncSent.put(target, true);
    } else {
      return true;
    }
    return needsFurtherProgress;
  }
}


