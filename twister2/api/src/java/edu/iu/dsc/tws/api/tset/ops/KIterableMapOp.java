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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.CacheableImpl;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.KIterableMapFunction;

public class KIterableMapOp<K, V, O> implements ICompute, Receptor {
  private static final long serialVersionUID = -1220168533L;

  private KIterableMapFunction<K, V, O> mapFn;

  private TaskContext context;

  private boolean inputIterator;

  private boolean keyed;

  private Selector<K, O> keySelector;

  public KIterableMapOp() {
  }

  public KIterableMapOp(KIterableMapFunction<K, V, O> mapFn,
                        boolean inputItr, boolean kyd, Selector<K, O> keySelector) {
    this.mapFn = mapFn;
    this.inputIterator = inputItr;
    this.keyed = kyd;
    this.keySelector = keySelector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(IMessage content) {
    Iterable<Tuple<K, V>> data;
    if (inputIterator) {
      data = new TSetIterable<>((Iterator<Tuple<K, V>>) content.getContent());
    } else {
      ArrayList<Tuple<K, V>> itr = new ArrayList<>();
      itr.add((Tuple<K, V>) content.getContent());
      data = new TSetIterable<>(itr.iterator());
    }

    O result = mapFn.map(data);
    K key = this.keySelector.select(result);
    return context.write(Constants.DEFAULT_EDGE, key, result);
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
