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
package edu.iu.dsc.tws.arrow;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.ObjectBuilder;
import edu.iu.dsc.tws.api.comms.packing.PackerStore;

import io.netty.buffer.ArrowBuf;

public class ArrowTablePacker implements DataPacker<ArrowTable, ArrowBuf> {
  @Override
  public int determineLength(ArrowTable data, PackerStore store) {
    // lets create the schema and the
    VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(data.getColumns()));
    // store this
    store.put("root", root);
    // now lets create the batches
    VectorUnloader loader = new VectorUnloader(root);
    ArrowRecordBatch batch = loader.getRecordBatch();
    // store the batch
    store.put("batch", batch);

    int bodyLength = batch.computeBodyLength();
    Preconditions.checkArgument(bodyLength % 8 == 0, "batch is not aligned");

    ByteBuffer serializedMessage = MessageSerializer.serializeMetadata(batch);
    int metadataLength = serializedMessage.remaining();
    // store the serialized metadata
    store.put("metadata", serializedMessage);
    store.put("metadata-length", metadataLength);
    store.put("body-length", bodyLength);

    int prefixSize = 8;
    // calculate alignment bytes so that metadata length points to the correct
    // location after alignment
    int padding = (metadataLength + prefixSize) % 8;
    if (padding != 0) {
      metadataLength += 8 - padding;
    }

    return metadataLength + prefixSize + bodyLength;
  }

  @Override
  public void writeDataToBuffer(ArrowTable data, PackerStore packerStore,
                                int alreadyCopied, int leftToCopy,
                                int spaceLeft, ByteBuffer targetBuffer) {
    int metaLength = (int) packerStore.get("metadata-length");
    int totalCopied = alreadyCopied;
    int left = spaceLeft;

    // first write the
    if (alreadyCopied < metaLength) {
      ByteBuffer serializedMessage = (ByteBuffer) packerStore.get("metadata");
      int toCopy = metaLength - alreadyCopied;

      // we need to copy only to space left
      if (toCopy > left) {
        toCopy = left;
      }

      targetBuffer.put(serializedMessage.array(), 0, toCopy);
      totalCopied += toCopy;
      left -= toCopy;
    }

    if (totalCopied >= metaLength) {
      if (totalCopied == metaLength && left >= 4) {
        targetBuffer.putInt(MessageSerializer.IPC_CONTINUATION_TOKEN);
        totalCopied += 4;
        left -= 4;
      }

      if (left > 0 && leftToCopy - (totalCopied - alreadyCopied) > 0) {
        ArrowRecordBatch batch = (ArrowRecordBatch) packerStore.get("batch");
        List<ArrowBuf> buffers = batch.getBuffers();
        List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

        for (int i = 0; i < buffers.size(); i++) {
          ArrowBuf buffer = buffers.get(i);
          ArrowBuffer layout = buffersLayout.get(i);

          buffer.getBytes(i, targetBuffer);
        }
      }
    }
  }

  @Override
  public int readDataFromBuffer(ObjectBuilder<ArrowTable, ArrowBuf> objectBuilder,
                                int currentBufferLocation, DataBuffer dataBuffer) {

    return 0;
  }

  @Override
  public byte[] packToByteArray(ArrowTable data) {
    return new byte[0];
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, ArrowTable data) {
    return null;
  }

  @Override
  public ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, ArrowTable data) {
    return null;
  }

  @Override
  public ArrowBuf wrapperForByteLength(int byteLength) {
    return null;
  }

  @Override
  public boolean isHeaderRequired() {
    return false;
  }

  @Override
  public ArrowTable unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    return null;
  }

  @Override
  public ArrowTable unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    return null;
  }
}
