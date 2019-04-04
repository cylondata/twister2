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
package edu.iu.dsc.tws.api.tset.link;

import java.util.ArrayList;
import java.util.List;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.common.config.Config;

public abstract class BaseTLink<T> implements TLink<T> {

  /**
   * The children of this set
   */
  protected List<TBase<?>> children;

  /**
   * The TSet Env used for runtime operations
   */
  protected TSetEnv tSetEnv;


  /**
   * Name of the data set
   */
  protected String name;

  /**
   * The parallelism of the set
   */
  protected int parallel = 4;
  /**
   * The configuration
   */
  protected Config config;

  public BaseTLink(Config cfg, TSetEnv tSetEnv) {
    this.children = new ArrayList<>();
    this.tSetEnv = tSetEnv;
    this.config = cfg;
  }

  @Override
  public BaseTLink<T> setName(String n) {
    this.name = n;
    return this;
  }

  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallel;
  }

  @Override
  public void build() {
// first build our selves
    baseBuild();

    // then build children
    for (TBase<?> c : children) {
      c.build();
    }
  }

  protected Class getType() {
    TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    return typeToken.getRawType();
  }

  /**
   * Override the parallelism
   *
   * @return if overide, return value, otherwise -1
   */
  public int overrideParallelism() {
    return -1;
  }

  public List<TBase<?>> getChildren() {
    return children;
  }

}
