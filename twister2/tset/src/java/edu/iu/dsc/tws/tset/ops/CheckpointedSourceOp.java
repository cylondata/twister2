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
package edu.iu.dsc.tws.tset.ops;

import java.util.Map;

import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSourceWrapper;

public class CheckpointedSourceOp<T> extends SourceOp<T> implements Collector {
  private DiskPartitionBackedSourceWrapper<T> sourceWrapper;
  private IONames collectible;

  public CheckpointedSourceOp(DiskPartitionBackedSourceWrapper<T> sourceWrapper,
                              BaseTSet originTSet, Map<String, String> receivables) {
    super(sourceWrapper, originTSet, receivables);
    this.sourceWrapper = sourceWrapper;
    this.collectible = IONames.declare(originTSet.getId());
  }

  @Override
  public DataPartition<?> get(String name) {
    return sourceWrapper.get();
  }

  @Override
  public IONames getCollectibleNames() {
    return collectible;
  }
}
