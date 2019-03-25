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

import java.util.Map;

public abstract class TBaseFunction implements TFunction {
  /**
   * The runtime context that is made avilable to users who create functions that
   * extend from the TBaseFunction abstract class
   */
  public TSetContext context = new TSetContext();

  @Override
  public void prepare(TSetContext ctx) {
    this.context.setConfig(ctx.getConfig());
    this.context.setParallelism(ctx.getParallelism());
    this.context.settSetId(ctx.getId());
    this.context.settSetIndex(ctx.getIndex());
    this.context.settSetName(ctx.getName());
    this.context.setWorkerId(ctx.getWorkerId());
    this.context.getInputMap().putAll(ctx.getInputMap());
    prepare();
  }

  public abstract void prepare();

  /**
   * Gets the input value for the given key from the input map
   *
   * @param key the key to be retrieved
   * @return the object associated with the given key, null if the key is not present
   */
  @Override
  public Object getInput(String key) {
    return context.getInputMap().get(key);
  }

  /**
   * Adds the given key value pair into the input map
   *
   * @param key the key to be added
   * @param input the value associated with the key
   */
  @Override
  public void addInput(String key, Cacheable<?> input) {
    context.getInputMap().put(key, input);
  }

  /**
   * Adds the given map into the input map
   *
   * @param map map that contains key, input pairs that need to be added into
   * the  input map
   */
  @Override
  public void addInputs(Map<String, Cacheable<?>> map) {
    context.getInputMap().putAll(map);
  }
}
