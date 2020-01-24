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

package edu.iu.dsc.tws.tset.sets;

import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;

public abstract class BaseTSetWithSchema<T> extends BaseTSet<T> {
  private Schema outSchema;
  private Schema inSchema;

  public BaseTSetWithSchema() {
  }

//  /**
//   * Special constructor to be used by sources which don't have input schema
//   */
//  public BaseTSetWithSchema(TSetEnvironment env, String n, int parallel) {
//    this(env, n, parallel, PrimitiveSchemas.NULL);
//  }

  /**
   * General constructor for {@link TSet}s
   *
   * @param env         env
   * @param name        name
   * @param parallel    par
   * @param inputSchema Schema from the preceding {@link edu.iu.dsc.tws.api.tset.link.TLink}
   */
  public BaseTSetWithSchema(TSetEnvironment env, String name, int parallel, Schema inputSchema) {
    this(env, name, parallel, inputSchema, PrimitiveSchemas.OBJECT);
  }

  public BaseTSetWithSchema(TSetEnvironment env, String name, int parallel, Schema inputSchema,
                            Schema outputSchema) {
    super(env, name, parallel);
    this.inSchema = inputSchema;
    this.outSchema = outputSchema;
  }

  protected void setOutputSchema(Schema outputSchema) {
    this.outSchema = outputSchema;
  }

  public Schema getOutputSchema() {
    return this.outSchema;
  }

  public Schema getInputSchema() {
    return this.inSchema;
  }
}
