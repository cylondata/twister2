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
package edu.iu.dsc.tws.dataset.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataPartitionConsumer;

public class CollectionPartition<T> implements DataPartition<T> {
  private List<T> dataList = new ArrayList<>();

  private int id;

  public CollectionPartition(int id) {
    this.id = id;
  }

  public void add(T val) {
    dataList.add(val);
  }

  public void addAll(Collection<T> vals) {
    dataList.addAll(vals);
  }

  @Override
  public DataPartitionConsumer<T> getConsumer() {
    return new IterativeConsumer<>(dataList.iterator());
  }

  @Override
  public int getPartitionId() {
    return id;
  }
}
