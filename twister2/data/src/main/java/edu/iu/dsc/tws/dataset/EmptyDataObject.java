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
package edu.iu.dsc.tws.dataset;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;

// todo: every time a new empty object will be created! find a design pattern that would avoid
//  this!
public final class EmptyDataObject<T> implements DataObject<T> {
  private static final String EMPTY = "EMPTY_DATAOBJECT";
  private DataPartition<T> emptyPartition = new DataPartition<T>() {
    @Override
    public DataPartitionConsumer<T> getConsumer() {
      return new DataPartitionConsumer<T>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public T next() {
          return null;
        }
      };
    }

    @Override
    public int getPartitionId() {
      return 0;
    }
  };

  private DataPartition[] partitions = new DataPartition[]{emptyPartition};

  @Override
  public void addPartition(DataPartition<T> partition) {
  }

  @Override
  public DataPartition<T>[] getPartitions() {
    return partitions;
  }

  @Override
  public DataPartition<T> getPartition(int partitionId) {
    return emptyPartition;
  }

  @Override
  public int getPartitionCount() {
    return partitions.length;
  }

  @Override
  public String getID() {
    return EMPTY;
  }
}
