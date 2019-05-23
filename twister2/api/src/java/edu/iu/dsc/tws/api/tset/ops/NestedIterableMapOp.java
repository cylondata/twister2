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

import java.util.ArrayList;
import java.util.Iterator;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.tset.CacheableImpl;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.NestedIterableMapFunction;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class NestedIterableMapOp<K, V, O> implements ICompute, Receptor {
  private static final long serialVersionUID = -1220168533L;

  private NestedIterableMapFunction<K, V, O> mapFn;

  private TaskContext context;

  private boolean inputIterator;

  private boolean keyed;

  public NestedIterableMapOp() {
  }

  public NestedIterableMapOp(NestedIterableMapFunction<K, V, O> mapFn, boolean inputItr,
                             boolean kyd) {
    this.mapFn = mapFn;
    this.inputIterator = inputItr;
    this.keyed = kyd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(IMessage content) {
    Iterable<Tuple<K, Iterable<V>>> data;
    if (inputIterator) {
      data = new TSetIterable<>((Iterator<Tuple<K, Iterable<V>>>) content.getContent());
    } else {
      ArrayList<Tuple<K, Iterable<V>>> itr = new ArrayList<>();
      itr.add((Tuple<K, Iterable<V>>) content.getContent());
      data = new TSetIterable<>(itr.iterator());
    }

    O result = mapFn.map(data);
    return context.write(Constants.DEFAULT_EDGE, result);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    TSetContext tSetContext = new TSetContext(cfg, ctx.taskIndex(), ctx.globalTaskId(),
        ctx.taskName(), ctx.getParallelism(), ctx.getWorkerId(), ctx.getConfigurations());

    mapFn.prepare(tSetContext);
  }

  @Override
  public void add(String name, DataObject<?> data) {
    mapFn.addInput(name, new CacheableImpl<>(data));
  }
}
