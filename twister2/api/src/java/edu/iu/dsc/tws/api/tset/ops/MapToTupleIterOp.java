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


package edu.iu.dsc.tws.api.tset.ops;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.TFunction;

public class MapToTupleIterOp<K, O, I> extends BaseComputeOp<Iterator<I>> {
  private MapFunction<Tuple<K, O>, I> mapFunction;

  public MapToTupleIterOp(MapFunction<Tuple<K, O>, I> mapToTupFn) {
    this.mapFunction = mapToTupFn;
  }

  @Override
  public TFunction getFunction() {
    return mapFunction;
  }

  @Override
  public boolean execute(IMessage<Iterator<I>> content) {
    Iterator<I> input = content.getContent();
    while (input.hasNext()) {
      Tuple<K, O> tuple = mapFunction.map(input.next());
      keyedWriteToEdges(tuple.getKey(), tuple.getValue());
    }

    writeEndToEdges();
    return false;
  }
}
