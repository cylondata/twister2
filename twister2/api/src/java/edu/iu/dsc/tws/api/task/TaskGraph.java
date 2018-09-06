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
package edu.iu.dsc.tws.api.task;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.ISink;

public class TaskGraph {
  private static final Logger LOG = Logger.getLogger(TaskGraph.class.getName());

  private Config config;

  public TaskGraph(Config cfg) {
    this.config = cfg;
  }

  public SinkConnection addSink(String name, ISink sink) {
    return null;
  }

  public SinkConnection addSink(String name, ISink sink, int parallel) {
    return null;
  }

  public ComputeConnection addCompute(String name, ICompute compute) {
    return null;
  }

  public ComputeConnection addCompute(String name, ICompute compute, int parallel) {
    return null;
  }

  public SourceConnection addSource(String name, ISink sink) {
    return null;
  }
}
