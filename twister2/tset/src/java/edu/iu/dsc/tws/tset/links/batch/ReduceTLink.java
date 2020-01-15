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

package edu.iu.dsc.tws.tset.links.batch;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;

public class ReduceTLink<T> extends BatchSingleLink<T> {
  private ReduceFunc<T> reduceFn;

  public ReduceTLink(BatchTSetEnvironment tSetEnv, ReduceFunc<T> rFn, int sourceParallelism) {
    super(tSetEnv, "reduce", sourceParallelism, 1);
    this.reduceFn = rFn;
  }

  @Override
  public ReduceTLink<T> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public ReduceTLink<T> withDataType(MessageType dataType) {
    return (ReduceTLink<T>) super.withDataType(dataType);
  }

  @Override
  public Edge getEdge() {
    return new Edge(getId(), OperationNames.REDUCE, getDataType(), reduceFn);
  }

}
