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
package edu.iu.dsc.tws.tset.fn.impl;

import java.util.List;

import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;

public class ListBasedSourceFunction<T> implements SourceFunc<T> {

  private String listName;
  private List<T> dataList;
  private int index;
  private int endIndex;

  public ListBasedSourceFunction(String listName) {
    this.listName = listName;
  }

  @Override
  public void prepare(TSetContext context) {
    this.dataList = WorkerEnvironment.getSharedValue(listName, List.class);
    if (this.dataList != null) {
      int parallelism = context.getParallelism();
      int chunk = (this.dataList.size() / parallelism) + 1;
      index = chunk * context.getIndex();
      endIndex = Math.min(index + chunk, dataList.size());
    }
  }

  @Override
  public boolean hasNext() {
    return this.dataList != null && index < endIndex;
  }

  @Override
  public T next() {
    return this.dataList.get(index++);
  }
}
