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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class ComputeCollectorToTupleOp<K, O, I> extends BaseComputeOp<I> {
  private ComputeCollectorFunc<Tuple<K, O>, I> compFunction;
  private RecordCollector<Tuple<K, O>> collector;

  public ComputeCollectorToTupleOp() {
  }

  public ComputeCollectorToTupleOp(ComputeCollectorFunc<Tuple<K, O>, I> mapToTupFn,
                                   BaseTSet origin, Map<String, String> receivables) {
    super(origin, receivables);
    this.compFunction = mapToTupFn;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.collector = new RecordCollectorImpl<>();
  }

  @Override
  public TFunction getFunction() {
    return compFunction;
  }

  @Override
  public boolean execute(IMessage<I> content) {
    compFunction.compute(content.getContent(), collector);
    return false;
  }

  private class RecordCollectorImpl<K, O> implements RecordCollector<Tuple<K, O>> {
    @Override
    public void collect(Tuple<K, O> record) {
      keyedWriteToEdges(record.getKey(), record.getValue());
    }

    @Override
    public void close() {

    }
  }

  @Override
  public void endExecute() {
    compFunction.end();
    writeEndToEdges();
  }

  @Override
  public void close() {
    compFunction.close();
  }
}
