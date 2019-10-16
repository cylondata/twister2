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
package edu.iu.dsc.tws.api.dataset;

public final class EmptyDataPartition implements DataPartition {
  private EmptyDataPartition() {
  }

  @Override
  public DataPartitionConsumer getConsumer() {
    return new DataPartitionConsumer() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Object next() {
        return null;
      }
    };
  }

  @Override
  public void setId(int id) {

  }

  @Override
  public int getPartitionId() {
    return -1;
  }

  private static class BillPughSingleton {
    private static final EmptyDataPartition INSTANCE = new EmptyDataPartition();
  }

  public static EmptyDataPartition getInstance() {
    return BillPughSingleton.INSTANCE;
  }
}
