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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public abstract class BufferedCollectionPartition<T> extends CollectionPartition<T>
    implements Closeable {

  private static final long DEFAULT_MAX_BUFFERED_BYTES = 10000000;
  private static final MessageType DEFAULT_DATA_TYPE = MessageTypes.OBJECT;

  private static final String EXTENSION = ".pbck";

  private long maxFramesInMemory;
  private MessageType dataType;

  private List<Path> filesList = new ArrayList<>();
  private long fileCounter;

  private Queue<byte[]> buffers = new LinkedList<>();
  private long bufferedBytes = 0;
  private long maxBufferedBytes;

  private FileSystem fileSystem;
  private Path rootPath;

  private String reference;

  /**
   * Creates an instance of {@link BufferedCollectionPartition}
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   * @param dataType {@link MessageType} of data frames
   * @param bufferedBytes amount to buffer in memory before writing to the disk
   */
  public BufferedCollectionPartition(long maxFramesInMemory, MessageType dataType,
                                     long bufferedBytes, Config config, String reference) {
    super();
    this.reference = reference;
    this.maxFramesInMemory = maxFramesInMemory;
    this.maxBufferedBytes = bufferedBytes;
    this.dataType = dataType;
    try {
      this.fileSystem = getFileSystem(config);
      this.rootPath = getRootPath(config);
      this.fileSystem.mkdirs(this.rootPath);
    } catch (IOException e) {
      throw new Twister2RuntimeException(
          "Failed to initialize and create a directory to hold the partition", e);
    }
  }

  /**
   * Create a disk based partition with default buffer size(10MB) and without a {@link MessageType}
   * If {@link MessageType} is not defined, it will assume as
   * {@link edu.iu.dsc.tws.api.comms.messaging.types.ObjectType}. This will
   * make serialization and deserialization inefficient.
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   */
  public BufferedCollectionPartition(long maxFramesInMemory, Config config) {
    this(maxFramesInMemory, DEFAULT_DATA_TYPE, DEFAULT_MAX_BUFFERED_BYTES,
        config, UUID.randomUUID().toString());
  }

  /**
   * Creates an instance with default bugger size 10MB
   *
   * @param maxFramesInMemory No of frames(elements) to keep in memory
   * @param dataType {@link MessageType} of data frames
   */
  public BufferedCollectionPartition(long maxFramesInMemory,
                                     MessageType dataType, Config config) {
    this(maxFramesInMemory, dataType, DEFAULT_MAX_BUFFERED_BYTES, config,
        UUID.randomUUID().toString());
  }

  /**
   * Creates an 100% disk based instance. This mode will not keep any data frame in memory
   *
   * @param dataType {@link MessageType} of data frames
   * @param bufferedBytes amount to buffer in memory before writing to the disk
   */
  public BufferedCollectionPartition(MessageType dataType,
                                     long bufferedBytes, Config config) {
    this(0, dataType, bufferedBytes, config, UUID.randomUUID().toString());
  }

  /**
   * Loads an disk based partition from file system.
   * This mode will not keep any data frame in memory.
   *
   * @param dataType {@link MessageType} of data frames
   * @param bufferedBytes amount to buffer in memory before writing to the disk
   * @param reference Partition reference
   */
  public BufferedCollectionPartition(MessageType dataType,
                                     long bufferedBytes, Config config, String reference) {
    this(0, dataType, bufferedBytes, config, reference);
    this.loadFromFS();
  }

  protected abstract FileSystem getFileSystem(Config config) throws IOException;

  protected abstract Path getRootPath(Config config);

  /**
   * This method loads existing frames on disk
   */
  private void loadFromFS() {
    try {
      FileStatus[] fileStatuses = this.fileSystem.listFiles(this.rootPath);
      this.filesList = Arrays.stream(fileStatuses).map(FileStatus::getPath)
          .filter(p -> p.getName().contains(EXTENSION))
          .sorted(Comparator.comparingLong(path ->
              Long.parseLong(path.getName().replace(EXTENSION, ""))))
          .collect(Collectors.toList());
      this.fileCounter = fileStatuses.length;
    } catch (IOException e) {
      throw new Twister2RuntimeException("Failed to load frames from file system", e);
    }
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
  public void addAll(Collection<T> frames) {
    for (T frame : frames) {
      this.add(frame);
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
            DataInputStream reader = new DataInputStream(fileSystem.open(nextFile));
            long noOfFrames = reader.readLong();
            for (long i = 0; i < noOfFrames; i++) {
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
        this.fileSystem.delete(path, true);
      } catch (IOException e) {
        throw new Twister2RuntimeException(
            "Failed to delete the temporary file : " + path.toString(), e);
      }
    }

    //cleanup memory
    this.filesList.clear();
    this.buffers.clear();
    this.bufferedBytes = 0;
    this.fileCounter = 0;
  }

  public void flush() {
    Path filePath = new Path(this.rootPath, (this.fileCounter++) + EXTENSION);
    try (DataOutputStream outputStream = new DataOutputStream(this.fileSystem.create(filePath))) {
      outputStream.writeLong(this.buffers.size());
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

  @Override
  public void close() {
    this.flush();
  }

  @Override
  public String getReference() {
    return this.reference;
  }
}
