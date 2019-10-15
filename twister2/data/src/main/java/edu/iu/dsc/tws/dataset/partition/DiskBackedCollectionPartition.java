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
package edu.iu.dsc.tws.dataset.partition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class DiskBackedCollectionPartition<T> extends CollectionPartition<T> {

  private int maxFramesInMemory;
  private MessageType dataType = MessageTypes.OBJECT;

  private List<Path> filesList = new ArrayList<>();
  private Path tempDirectory;

  private Queue<byte[]> buffers = new LinkedList<>();
  private long bufferedBytes = 0;
  private long maxBufferedBytes = 10000000; // 10mb

  /**
   * Create a disk based partition with default buffer size(10MB) and without a {@link MessageType}
   * If {@link MessageType} is not defined, it will assume as
   * {@link edu.iu.dsc.tws.api.comms.messaging.types.ObjectType}. This will
   * make serialization and deserialization inefficient.
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   */
  public DiskBackedCollectionPartition(int maxFramesInMemory) {
    super();
    this.maxFramesInMemory = maxFramesInMemory;
    try {
      this.tempDirectory = Files.createTempDirectory(UUID.randomUUID().toString());
    } catch (IOException e) {
      throw new Twister2RuntimeException(
          "Failed to create a temp directory to hold the partition", e);
    }
  }

  /**
   * Creates an instance with default bugger size 10MB
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   * @param dataType {@link MessageType} of data frames
   */
  public DiskBackedCollectionPartition(int maxFramesInMemory, MessageType dataType) {
    this(maxFramesInMemory);
    this.dataType = dataType;
  }

  /**
   * Creates an instance of {@link DiskBackedCollectionPartition}
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   * @param dataType {@link MessageType} of data frames
   * @param bufferedBytes amount to buffer in memory before writing to the disk
   */
  public DiskBackedCollectionPartition(int maxFramesInMemory, MessageType dataType,
                                       int bufferedBytes) {
    this(maxFramesInMemory, dataType);
    this.bufferedBytes = bufferedBytes;
  }

  /**
   * Creates an 100% disk based instance. This mode will not keep any data frame in memory
   *
   * @param dataType {@link MessageType} of data frames
   * @param bufferedBytes amount to buffer in memory before writing to the disk
   */
  public DiskBackedCollectionPartition(MessageType dataType,
                                       int bufferedBytes) {
    this(0, dataType);
    this.bufferedBytes = bufferedBytes;
  }

  @Override
  public void add(T val) {
    if (this.dataList.size() < this.maxFramesInMemory) {
      super.add(val);
    } else {
      // write to buffer
      byte[] bytes = dataType.getDataPacker().packToByteArray(val);
      this.buffers.add(bytes);
      this.bufferedBytes += bytes.length;

      // flush to disk
      if (this.bufferedBytes > this.maxBufferedBytes) {
        this.flush();
      }
    }
  }

  @Override
  public void addAll(Collection<T> vals) {
    for (T val : vals) {
      this.add(val);
    }
  }

  @Override
  public DataPartitionConsumer<T> getConsumer() {

    final Iterator<T> inMemoryIterator = this.dataList.iterator();
    final Iterator<Path> fileIterator = this.filesList.iterator();
    final Iterator<byte[]> buffersIterator = this.buffers.iterator();

    return new DataPartitionConsumer<T>() {

      private Queue<byte[]> bufferFromDisk = new LinkedList<>();

      @Override
      public boolean hasNext() {
        return inMemoryIterator.hasNext()
            || fileIterator.hasNext() || buffersIterator.hasNext() || !bufferFromDisk.isEmpty();
      }

      @Override
      public T next() {
        if (!this.bufferFromDisk.isEmpty()) {
          return (T) dataType.getDataPacker().unpackFromByteArray(this.bufferFromDisk.poll());
        } else if (inMemoryIterator.hasNext()) {
          return inMemoryIterator.next();
        } else if (fileIterator.hasNext()) {
          Path nextFile = fileIterator.next();
          try {
            DataInputStream reader = new DataInputStream(Files.newInputStream(nextFile));
            int noOfFrames = reader.readInt();
            for (int i = 0; i < noOfFrames; i++) {
              int size = reader.readInt();
              byte[] data = new byte[size];
              reader.read(data);
              this.bufferFromDisk.add(data);
            }
            return next();
          } catch (IOException e) {
            throw new Twister2RuntimeException(
                "Failed to read value from the temp file : " + nextFile.toString(), e);
          }
        } else if (buffersIterator.hasNext()) {
          return (T) dataType.getDataPacker().unpackFromByteArray(buffersIterator.next());
        }
        throw new Twister2RuntimeException("No more frames available in this partition");
      }
    };
  }

  @Override
  public void clear() {
    // cleanup files
    for (Path path : this.filesList) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        throw new Twister2RuntimeException(
            "Failed to delete the temporary file : " + path.toString(), e);
      }
    }
  }

  public void flush() {
    Path filePath = Paths.get(tempDirectory.toString(), UUID.randomUUID().toString());
    try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(filePath))) {
      outputStream.writeInt(this.buffers.size());
      while (!this.buffers.isEmpty()) {
        byte[] next = this.buffers.poll();
        outputStream.writeInt(next.length);
        outputStream.write(next);
      }
    } catch (IOException e) {
      throw new Twister2RuntimeException("Couldn't flush partitions to the disk", e);
    }
    this.filesList.add(filePath);
    this.bufferedBytes = 0;
  }
}
