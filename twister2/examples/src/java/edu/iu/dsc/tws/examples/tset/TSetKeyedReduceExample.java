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
package edu.iu.dsc.tws.examples.tset;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.IdentitySelector;
import edu.iu.dsc.tws.api.tset.fn.LoadBalancePartitioner;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class TSetKeyedReduceExample extends BaseTSetWorker {
  private static final Logger LOG = Logger.getLogger(TSetKeyedReduceExample.class.getName());

  @Override
  public void execute() {
    super.execute();

    // set the parallelism of source to task stage 0
    TSet<int[]> source = tSetBuilder.createSource(new BaseSource()).setName("Source").
        setParallelism(jobParameters.getTaskStages().get(0));
    TSet<int[]> reduce = source.groupBy(new LoadBalancePartitioner<>(), new IdentitySelector<>()).
        keyedReduce((t1, t2) -> {
          int[] val = new int[t1.length];
          for (int i = 0; i < t1.length; i++) {
            val[i] = t1[i] + t2[i];
          }
          return val;
        }).setParallelism(10);

    reduce.sink(new Sink<int[]>() {
      @Override
      public boolean add(int[] value) {
        experimentData.setOutput(value);
        try {
          verify(OperationNames.REDUCE);
        } catch (VerificationException e) {
          LOG.info("Exception Message : " + e.getMessage());
        }
        return true;
      }

      @Override
      public void prepare(TSetContext context) {
      }
    });

    DataFlowTaskGraph graph = tSetBuilder.build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, executionPlan);
  }

}
