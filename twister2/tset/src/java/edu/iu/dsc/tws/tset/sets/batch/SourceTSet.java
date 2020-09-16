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

import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.SourceOp;

public class SourceTSet<T> extends BatchTSetImpl<T> {
  private SourceFunc<T> source;

  public SourceTSet() {
    //non arg constructor needed for kryo
  }

  public SourceTSet(BatchEnvironment tSetEnv, SourceFunc<T> src, int parallelism) {
    this(tSetEnv, "source", src, parallelism);
  }

  public SourceTSet(BatchEnvironment tSetEnv, String name, SourceFunc<T> src, int parallelism) {
    super(tSetEnv, name, parallelism, PrimitiveSchemas.NULL);
    this.source = src;
  }

  @Override
  public SourceTSet<T> addInput(String key, StorableTBase<?> input) {
    return (SourceTSet<T>) super.addInput(key, input);
  }

  @Override
  public SourceTSet<T> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public SourceTSet<T> withSchema(Schema schema) {
    return (SourceTSet<T>) super.withSchema(schema);
  }

  @Override
  public INode getINode() {
    return new SourceOp<>(source, this, getInputs());
  }
}
