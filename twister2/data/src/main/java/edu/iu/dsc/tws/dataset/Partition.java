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

import java.util.Iterator;

public class Partition<T> implements PSet<T> {
  private T data;

  private int id;

  private int workerId;

  public Partition(int id) {
    this.id = id;
  }

  public Partition(int pId, T d) {
    this.data = d;
    this.id = pId;
  }

  public T getData() {
    return data;
  }

  public int getId() {
    return id;
  }

  @Override
  public int getWorkerId() {
    return id;
  }

  @Override
  public int getPartitionId() {
    return 0;
  }

  @Override
  public Iterator<T> iterator() {
    return null;
  }
}
