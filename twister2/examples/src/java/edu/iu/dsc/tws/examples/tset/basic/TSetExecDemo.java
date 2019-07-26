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

package edu.iu.dsc.tws.examples.tset.basic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.OperationNames;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.ForEachIterCompute;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.ops.ComputeOp;
import edu.iu.dsc.tws.api.tset.ops.SourceOp;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.TaskEnvironment;
import edu.iu.dsc.tws.task.graph.GraphBuilder;

public class TSetExecDemo implements IWorker, Serializable {
  static final int COUNT = 5;
  static final int PARALLELISM = 2;
  private static final Logger LOG = Logger.getLogger(TSetExecDemo.class.getName());


  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    WorkerEnvironment env = WorkerEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);

    TaskEnvironment tenv = TaskEnvironment.init(env);

    GraphBuilder graph = GraphBuilder.newBuilder();
    graph.operationMode(OperationMode.BATCH);

    graph.addSource("src", new SourceOp<>(new SourceFunc<Integer>() {
      private int c = 0;

      @Override
      public boolean hasNext() {
        return c < COUNT;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }), PARALLELISM);


    graph.addTask("compute",
        new ComputeOp<>((ComputeFunc<String, Iterator<Tuple<Integer, Integer>>>)
            input -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next().getValue();
              }
              LOG.info("####" + sum);
              return "sum=" + sum;
            }), 1);

    graph.connect("src", "compute", "e1", OperationNames.GATHER);

    graph.addTask("foreach",
        new ComputeOp<>(new ForEachIterCompute<>(s -> LOG.info("compute: " + s))), 1);

    graph.connect("compute", "foreach", "e2", OperationNames.DIRECT);

    DataFlowTaskGraph build = graph.build();
    ExecutionPlan plan = tenv.getTaskExecutor().plan(build);

    tenv.getTaskExecutor().execute(build, plan);
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig, TSetExecDemo.class.getName());
  }
}
