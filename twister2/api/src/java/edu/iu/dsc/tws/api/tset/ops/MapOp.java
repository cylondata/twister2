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
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.CacheableImpl;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;

public class MapOp<T, R> implements ICompute, Receptor {
  private static final long serialVersionUID = -1220168533L;

  private MapFunction<T, R> mapFn;

  private TaskContext context;

  private boolean iterable;

  private boolean keyed;

  public MapOp() {
  }

  public MapOp(MapFunction<T, R> mapFn, boolean itr, boolean kyd) {
    this.mapFn = mapFn;
    this.iterable = itr;
    this.keyed = kyd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(IMessage content) {
    if (!keyed) {
      if (!iterable) {
        T data = (T) content.getContent();
        R result = mapFn.map(data);
        return context.write(Constants.DEFAULT_EDGE, result);
      } else {
        Iterator<T> data = (Iterator<T>) content.getContent();
        while (data.hasNext()) {
          R result = mapFn.map(data.next());
          context.write(Constants.DEFAULT_EDGE, result);
        }
      }
    } else {
      if (!iterable) {
        Tuple data = (Tuple) content.getContent();
        R result = mapFn.map((T) data.getValue());
        return context.write(Constants.DEFAULT_EDGE, result);
      } else {
        Iterator<Tuple> data = (Iterator<Tuple>) content.getContent();
        while (data.hasNext()) {
          R result = mapFn.map((T) data.next().getValue());
          context.write(Constants.DEFAULT_EDGE, result);
        }
      }
    }
    return true;
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
