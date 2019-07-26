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
package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;

public abstract class BaseTupleTSet<K, V, T> implements TupleTSet<K, V, T> {

  /**
   * The TSet Env to use for runtime operations of the Tset
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  /**
   * The parallelism of the set
   */
  private int parallelism;

  public BaseTupleTSet(TSetEnvironment tSetEnv, String name) {
    this(tSetEnv, name, tSetEnv.getDefaultParallelism());
  }

  public BaseTupleTSet(TSetEnvironment env, String n, int parallel) {
    this.tSetEnv = env;
    this.name = n;
    this.parallelism = parallel;
  }

  protected void rename(String n) {
    this.name = n;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  public TSetEnvironment getTSetEnv() {
    return tSetEnv;
  }

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  @Override
  public String toString() {
    return "TupleTset{" + getName() + "[" + getParallelism() + "]}";
  }
}
