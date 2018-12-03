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

import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class FlatMapOp<T, R> implements ICompute {
  private static final long serialVersionUID = -5244396518L;

  private FlatMapFunction<T, R> mapFn;

  private TaskContext context;

  private CollectorImpl<R> collector;

  private boolean iterable;

  public FlatMapOp() {
  }

  public FlatMapOp(FlatMapFunction<T, R> mapFn, boolean itr) {
    this.mapFn = mapFn;
    this.iterable = itr;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(IMessage content) {
    if (!iterable) {
      T data = (T) content.getContent();
      mapFn.flatMap(data, collector);
    } else {
      Iterator<T> data = (Iterator<T>) content.getContent();
      while (data.hasNext()) {
        mapFn.flatMap(data.next(), collector);
      }
    }

    if (collector.isClosed()) {
      if (!collector.hasPending()) {
        context.end(Constants.DEFAULT_EDGE);
      }
    }
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    this.collector = new CollectorImpl<>(context, Constants.DEFAULT_EDGE);
  }
}
