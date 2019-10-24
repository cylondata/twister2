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

public final class EmptyDataObject implements DataObject {
  private static final String EMPTY = "__EMPTY_DATAOBJECT";

  private EmptyDataObject() {
  }

  @Override
  public void addPartition(DataPartition partition) {
  }

  @Override
  public DataPartition[] getPartitions() {
    return new DataPartition[0];
  }

  @Override
  public DataPartition getPartition(int partitionId) {
    return EmptyDataPartition.getInstance();
  }

  @Override
  public int getPartitionCount() {
    return 0;
  }

  @Override
  public String getID() {
    return EMPTY;
  }

  private static class BillPughSingleton {
    private static final EmptyDataObject INSTANCE = new EmptyDataObject();
  }

  public static DataObject getInstance() {
    return EmptyDataObject.BillPughSingleton.INSTANCE;
  }
}
