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
import java.util.Map;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;

public class MapBasedSourceFunction<K, V> implements SourceFunc<Tuple<K, V>> {

  private String listName;
  private String mapName;
  private Map<K, V> dataMap;
  private List<K> keysList;
  private int index, endIndex;

  public MapBasedSourceFunction(String listName, String mapName) {
    this.listName = listName;
    this.mapName = mapName;
  }

  @Override
  public void prepare(TSetContext context) {
    this.dataMap = WorkerEnvironment.getSharedValue(mapName, Map.class);
    this.keysList = WorkerEnvironment.getSharedValue(listName, List.class);
    int parallelism = context.getParallelism();
    int chunk = (this.keysList.size() / parallelism) + 1;
    index = chunk * context.getIndex();
    endIndex = Math.min(index + chunk, keysList.size());
  }

  @Override
  public boolean hasNext() {
    return this.dataMap != null && this.keysList != null && index < endIndex;
  }

  @Override
  public Tuple<K, V> next() {
    K key = this.keysList.get(index++);
    V value = this.dataMap.get(key);
    return Tuple.of(key, value);
  }
}
