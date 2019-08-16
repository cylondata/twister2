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

package edu.iu.dsc.tws.api.tset.link.streaming;

import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;

/**
 * Represent a data set create by a all reduce opration
 *
 * @param <T> type of data
 */
public class SAllReduceTLink<T> extends SSingleLink<T> {
  private ReduceFunc<T> reduceFn;

  public SAllReduceTLink(StreamingTSetEnvironment tSetEnv, ReduceFunc<T> rFn,
                         int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("sallreduce"), sourceParallelism);
    this.reduceFn = rFn;
  }

  @Override
  public Edge getEdge() {
    return new Edge(getId(), OperationNames.ALLREDUCE, getMessageType(), reduceFn);
  }

  @Override
  public SAllReduceTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
