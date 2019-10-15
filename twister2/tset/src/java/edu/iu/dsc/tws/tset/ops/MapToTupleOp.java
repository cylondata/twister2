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

import java.util.Set;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class MapToTupleOp<K, O, I> extends BaseComputeOp<I> {
  private MapCompute<Tuple<K, O>, I> mapFunction;

  public MapToTupleOp() {

  }

  public MapToTupleOp(MapCompute<Tuple<K, O>, I> mapToTupFn, BaseTSet origin,
                      Set<String> receivables) {
    super(origin, receivables);
    this.mapFunction = mapToTupFn;
  }

  @Override
  public TFunction getFunction() {
    return mapFunction;
  }

  @Override
  public boolean execute(IMessage<I> content) {
    Tuple<K, O> tuple = mapFunction.compute(content.getContent());
    keyedWriteToEdges(tuple.getKey(), tuple.getValue());
    writeEndToEdges();
    return false;
  }
}
