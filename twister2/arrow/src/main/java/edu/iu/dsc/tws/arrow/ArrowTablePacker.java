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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.ObjectBuilder;
import edu.iu.dsc.tws.api.comms.packing.PackerStore;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

import io.netty.buffer.ArrowBuf;

public class ArrowTablePacker implements DataPacker<ArrowTable, byte[]> {
  private VectorLoader loader;

  private VectorSchemaRoot root;

  private final BufferAllocator allocator;

  private VectorUnloader unloader;

  public ArrowTablePacker(Schema orginalSchema) {
    this.allocator = new RootAllocator();

    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    Map<Long, Dictionary> dictionaries = new HashMap<>();

    // Convert fields with dictionaries to have the index type
    for (Field field : orginalSchema.getFields()) {
      Field updated = DictionaryUtility.toMemoryFormat(field, allocator, dictionaries);
      fields.add(updated);
      vectors.add(updated.createVector(allocator));
    }
    Schema schema = new Schema(fields, orginalSchema.getCustomMetadata());

    this.root = new VectorSchemaRoot(schema, vectors, 0);
    this.loader = new VectorLoader(root);
    this.unloader = new VectorUnloader(root);
  }

  @Override
  public int determineLength(ArrowTable data, PackerStore store) {
    // now lets create the batches
    ArrowRecordBatch batch = unloader.getRecordBatch();
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

    if (totalCopied == 0 && left >= 4) {
      targetBuffer.putInt(MessageSerializer.IPC_CONTINUATION_TOKEN);
      totalCopied += 4;
      left -= 4;
    }

    if (totalCopied == 4 && left >= 4) {
      targetBuffer.putInt(metaLength);
      totalCopied += 4;
      left -= 4;
    }

    // write the metadata
    if (totalCopied < 8 + metaLength && left > 0) {
      ByteBuffer serializedMessage = (ByteBuffer) packerStore.get("metadata");
      // we can copy only to space left
      int toCopy = metaLength - alreadyCopied > left ? left : metaLength - alreadyCopied;
      targetBuffer.put(serializedMessage.array(), 0, toCopy);
      totalCopied += toCopy;
      left -= toCopy;
    }

    int padding = (metaLength + 8) % 8;
    long zeros = 8 - padding;
    // write the zeros to align to a 8 byte position
    if (totalCopied == metaLength + 8 && left >= zeros) {
      writeZerors(zeros, targetBuffer);
      totalCopied += zeros;
      left -= zeros;
    }

    // we use the counting size to go to the current buffer to write
    long countingSize = metaLength + 8 + zeros;
    long bufferStart = countingSize;
    if (totalCopied >= countingSize && left > 0) {
      ArrowRecordBatch batch = (ArrowRecordBatch) packerStore.get("batch");
      List<ArrowBuf> buffers = batch.getBuffers();
      List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();

      for (int i = 0; i < buffers.size(); i++) {
        ArrowBuf buffer = buffers.get(i);
        ArrowBuffer layout = buffersLayout.get(i);
        long startPosition = bufferStart + layout.getOffset();

        ByteBuffer nioBuffer =
            buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes());
        int size = nioBuffer.remaining();

        if (startPosition != countingSize) {
          // we need to align to 8 byte position
          zeros = startPosition - countingSize;

          if (totalCopied < countingSize + zeros && left > zeros) {
            writeZerors(zeros, targetBuffer);
            totalCopied += zeros;
            left -= zeros;
          }
          // weather we write or not we need to increment so we can go to the currect buffer
          countingSize += zeros;

          if (totalCopied < countingSize + size) {
            int currentTargetSize = targetBuffer.remaining();
            // now write the buffer
            buffer.getBytes(0, targetBuffer);
            int copied = targetBuffer.remaining() - currentTargetSize;

            totalCopied += copied;
            left -= copied;
          }
          countingSize += size;
        } else {
          // if we didn't write the zeros, lets write the buffer
          if (totalCopied < countingSize + size) {
            int currentTargetSize = targetBuffer.remaining();
            // now write the buffer
            buffer.getBytes(0, targetBuffer);
            int copied = targetBuffer.remaining() - currentTargetSize;

            totalCopied += copied;
            left -= copied;
          }
          // weather we write or not we need to increment so we can go to the currect buffer
          countingSize += size;
        }
      }

      if (countingSize % 8 != 0) {
        zeros = 8 - countingSize % 8;
        if (totalCopied < countingSize + zeros) {
          // we don't need to update the totaol copied and lef as this is the last
          writeZerors(zeros, targetBuffer);
        }
      }
    }
  }

  private void writeZerors(long number, ByteBuffer targetBuffer) {
    for (long i = 0; i < number; i++) {
      targetBuffer.put((byte) 0);
    }
  }

  @Override
  public int readDataFromBuffer(ObjectBuilder<ArrowTable, byte[]> objectBuilder,
                                int currentBufferLocation, DataBuffer dataBuffer) {
    int totalDataLength = objectBuilder.getTotalSize();
    int startIndex = objectBuilder.getCompletedSize();
    byte[] val = objectBuilder.getPartialDataHolder();

    ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
    int remainingInBuffer = dataBuffer.getSize() - currentBufferLocation;
    int leftToRead = totalDataLength - startIndex;

    int elementsToRead = Math.min(leftToRead, remainingInBuffer);

    byteBuffer.position(currentBufferLocation); //setting position for bulk read
    byteBuffer.get(val, startIndex, elementsToRead);

    if (totalDataLength == elementsToRead + startIndex) {
      try {
        ArrowRecordBatch batch = (ArrowRecordBatch) MessageSerializer.deserializeMessageBatch(
            new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(
                    objectBuilder.getPartialDataHolder(), 0, val.length))), allocator);

        // create the schema
        loader.load(batch);

        ArrowTable table = new ArrowTable(
            (FieldVector[]) root.getFieldVectors().toArray());
        objectBuilder.setFinalObject(table);
      } catch (IOException e) {
        throw new Twister2RuntimeException("Failed to read Arrow message", e);
      }
    }
    return elementsToRead;
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
  public byte[] wrapperForByteLength(int byteLength) {
    return new byte[byteLength];
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
