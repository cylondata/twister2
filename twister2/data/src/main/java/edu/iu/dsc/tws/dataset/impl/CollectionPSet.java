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
import java.util.Iterator;
import java.util.List;

import edu.iu.dsc.tws.dataset.PSet;
import edu.iu.dsc.tws.dataset.Partition;

public class CollectionPSet<T> extends Partition<T> implements PSet<T> {
  private List<T> dataList = new ArrayList<>();

  private int workerId;

  private int id;

  public CollectionPSet(int workerId, int id) {
    super(id);
    this.workerId = workerId;
    this.id = id;
  }

  @Override
  public int getWorkerId() {
    return workerId;
  }

  @Override
  public int getPartitionId() {
    return id;
  }

  @Override
  public Iterator<T> iterator() {
    return dataList.iterator();
  }

  public void add(T val) {
    dataList.add(val);
  }
}
