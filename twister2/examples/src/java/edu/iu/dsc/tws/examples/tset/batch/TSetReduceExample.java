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

import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TSetReduceExample extends BaseTSetWorker {
  @Override
  public void execute() {
    super.execute();

    TSet<int[]> source = tSetBuilder.createSource(new BaseSource()).setName("Source");
    TSet<int[]> reduce = source.reduce(new ReduceFunction<int[]>() {
      @Override
      public int[] reduce(int[] t1, int[] t2) {
        int[] val = new int[t1.length];
        for (int i = 0; i < t1.length; i++) {
          val[i] = t1[i] + t2[i];
        }
        return val;
      }

      @Override
      public void prepare(TSetContext context) {
      }
    });

    reduce.sink(new Sink<int[]>() {
      @Override
      public boolean add(int[] value) {
        for (int i = 0; i < value.length; i++) {
          System.out.println(value[i]);
        }
        return true;
      }

      @Override
      public void prepare(TSetContext context) {
      }
    });

    tSetBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = tSetBuilder.build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, executionPlan);
  }
}
