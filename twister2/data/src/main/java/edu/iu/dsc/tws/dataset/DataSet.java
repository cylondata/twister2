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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataSet<T> {
  private Map<Integer, Partition<T>> partitions = new HashMap<>();

  private int id;

  public DataSet(int dId) {
    this.id = dId;
  }

  public void addPartition(int dId, T data) {
    partitions.put(dId, new Partition<T>(dId, data));
  }

  public void addPartition(Partition<T> p) {
    partitions.put(p.getId(), p);
  }

  public T getPartition(int pId) {
    if (partitions.containsKey(pId)) {
      return partitions.get(pId).getData();
    } else {
      return null;
    }
  }

  public int getId() {
    return id;
  }

  public Set<T> getData() {
    Set<T> t = new HashSet<>();
    for (Map.Entry<Integer, Partition<T>> e : partitions.entrySet()) {
      t.add(e.getValue().getData());
    }
    return t;
  }
}
