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
package edu.iu.dsc.tws.examples.tset.batch;

import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetBuilder;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class HelloTSet extends TaskWorker {
  @Override
  public void execute() {
    TSetBuilder builder = TSetBuilder.newBuilder(config);

    TSet<String> source = builder.createSource(new Source<String>() {
      private int count = 0;
      @Override
      public boolean hasNext() {
        return count < 1;
      }

      @Override
      public String next() {
        count++;
        return "Hello - tset";
      }

      @Override
      public void prepare(TSetContext context) {
      }
    });

    source.sink(new Sink<String>() {
      @Override
      public boolean add(String value) {
        System.out.println("Sink");
        return false;
      }
    });

    DataFlowTaskGraph graph = builder.getBuilder().build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, executionPlan);
  }
}
