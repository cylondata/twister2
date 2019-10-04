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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

  public DiskBackedCollectionPartition(int maxFramesInMemory, MessageType dataType) {
    this(maxFramesInMemory);
    this.dataType = dataType;
  }

  @Override
  public void add(T val) {
    if (this.dataList.size() < this.maxFramesInMemory) {
      super.add(val);
    } else {
      //write to disk
      byte[] bytes = dataType.getDataPacker().packToByteArray(val);
      try {
        Path write = Files.write(
            Paths.get(tempDirectory.toString(), UUID.randomUUID().toString()), bytes);
        this.filesList.add(write);
      } catch (IOException e) {
        throw new Twister2RuntimeException("Failed to add data to the partition", e);
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
    return new DataPartitionConsumer<T>() {
      @Override
      public boolean hasNext() {
        return inMemoryIterator.hasNext() || fileIterator.hasNext();
      }

      @Override
      public T next() {
        if (inMemoryIterator.hasNext()) {
          return inMemoryIterator.next();
        } else {
          Path nextFile = fileIterator.next();
          try {
            byte[] bytes = Files.readAllBytes(nextFile);
            return (T) dataType.getDataPacker().unpackFromByteArray(bytes);
          } catch (IOException e) {
            throw new Twister2RuntimeException(
                "Failed to read value from the temp file : " + nextFile.toString(), e);
          }
        }
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
}
