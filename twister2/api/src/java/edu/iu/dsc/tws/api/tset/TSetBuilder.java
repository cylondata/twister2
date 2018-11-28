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

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.impl.SourceTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

/**
 * The builder for creating a tset graph.
 */
public final class TSetBuilder {
  private List<SourceTSet<?>> sources;

  private TSetBuilder(Config cfg) {
    this.config = cfg;
    this.sources = new ArrayList<>();
    this.builder = TaskGraphBuilder.newBuilder(cfg);
  }

  private Config config;

  private TaskGraphBuilder builder;

  public static TSetBuilder newBuilder(Config cfg) {
    return new TSetBuilder(cfg);
  }

  public <T> TSet<T> createSource(Source<T> source) {
    SourceTSet<T> tSourceTSet = new SourceTSet<>(config, builder, source);
    sources.add(tSourceTSet);
    return tSourceTSet;
  }

  public DataFlowTaskGraph build() {
    for (SourceTSet<?> set : sources) {
      set.build();
    }
    return builder.build();
  }
}
