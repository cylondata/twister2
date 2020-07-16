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


package edu.iu.dsc.tws.tset.sets.batch;

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.AcceptingData;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.SinkOp;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

public class SinkTSet<T> extends BaseTSetWithSchema<T> implements AcceptingData<T> {
  private SinkFunc<T> sinkFunc;

  /**
   * Creates SinkTSet with the given parameters, the parallelism of the TSet is taken as 1
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s       The Sink function to be used
   */
  public SinkTSet(BatchEnvironment tSetEnv, SinkFunc<T> s, Schema inputSchema) {
    this(tSetEnv, s, 1, inputSchema);
  }

  /**
   * Creates SinkTSet with the given parameters
   *
   * @param tSetEnv     The TSetEnv used for execution
   * @param sinkFn      The Sink function to be used
   * @param parallelism the parallelism of the sink
   */
  public SinkTSet(BatchEnvironment tSetEnv, SinkFunc<T> sinkFn, int parallelism,
                  Schema inputSchema) {
    super(tSetEnv, "sink", parallelism, inputSchema);
    this.sinkFunc = sinkFn;
  }

  @Override
  public ICompute getINode() {
    // If a sink function is producing a data object/ partition, it will be identified by the
    // TSet ID. Essentially, only one data object can be created by a TSet
    return new SinkOp<>(sinkFunc, this, getInputs());
  }

  @Override
  public SinkTSet<T> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public SinkTSet<T> addInput(String key, StorableTBase<?> input) {
    getTSetEnv().addInput(getId(), input.getId(), key);
    return this;
  }

  public SinkTSet<T> withSchema(Schema schema) {
    this.setOutputSchema(schema);
    return this;
  }
}
