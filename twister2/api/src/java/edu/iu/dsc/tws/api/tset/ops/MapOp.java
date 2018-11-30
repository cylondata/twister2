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

import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class MapOp<T, R> implements ICompute {
  private static final long serialVersionUID = -1220168533L;

  private MapFunction<T, R> mapFn;

  private TaskContext context;

  public MapOp() {
  }

  public MapOp(MapFunction<T, R> mapFn) {
    this.mapFn = mapFn;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(IMessage content) {
    T data = (T) content.getContent();
    R result = mapFn.map(data);
    return context.write(Constants.DEFAULT_EDGE, result);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
  }
}
