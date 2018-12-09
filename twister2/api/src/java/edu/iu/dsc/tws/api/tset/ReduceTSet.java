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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class ReduceTSet<T> extends BaseTSet<T> {
  private ReduceFunction<T> reduceFn;

  private BaseTSet<T> parent;

  public ReduceTSet(Config cfg, TaskGraphBuilder bldr,
                    BaseTSet<T> prnt, ReduceFunction<T> rFn) {
    super(cfg, bldr);
    this.reduceFn = rFn;
    this.parent = prnt;
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  void buildConnection(ComputeConnection connection) {
    DataType dataType = getDataType(getType());

    connection.reduce(parent.getName(), Constants.DEFAULT_EDGE,
        new ReduceOpFunction<T>(getReduceFn()), dataType);
  }

  public ReduceFunction<T> getReduceFn() {
    return reduceFn;
  }
}
