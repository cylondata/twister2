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

import edu.iu.dsc.tws.dataset.DataPartition;

public class EntityPartition<T> implements DataPartition<T, T> {
  private int id;

  private T value;

  public EntityPartition(int id, T val) {
    this.id = id;
    this.value = val;
  }

  @Override
  public int getPartitionId() {
    return id;
  }

  @Override
  public T getOut() {
    return value;
  }
}
