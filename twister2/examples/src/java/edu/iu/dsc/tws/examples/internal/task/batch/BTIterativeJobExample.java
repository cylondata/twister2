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
package edu.iu.dsc.tws.examples.internal.task.batch;

import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.examples.internal.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.internal.task.TaskExamples;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;


public class BTIterativeJobExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTIterativeJobExample.class.getName());

  @Override
  public void intialize() {
    isIterativeJob = true;
    List<Integer> taskStages = jobParameters.getTaskStages();
    int psource = taskStages.get(0);
    int psink = taskStages.get(1);
    Op operation = Op.SUM;
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    TaskExamples taskExamples = new TaskExamples();
    BaseBatchSource g = taskExamples.getBatchSourceClass("iterative-source", edge);
    BaseBatchSink r = taskExamples.getBatchSinkClass("iterative-sink");
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.reduce(SOURCE, edge, operation, dataType);
    dataFlowTaskGraph = taskGraphBuilder.build();
    for (int i = 0; i < 10; i++) {
      executionPlan = taskExecutor.plan(dataFlowTaskGraph);
      taskExecutor.addInput(dataFlowTaskGraph, executionPlan, SOURCE, "input", new DataSet<>(0));
      // this is a blocking call
      taskExecutor.execute(dataFlowTaskGraph, executionPlan);
      DataSet<Object> dataSet = taskExecutor.getOutput(dataFlowTaskGraph, executionPlan, SINK);
      Set<Object> values = dataSet.getData();
      LOG.log(Level.INFO, "Values: " + values);
    }
  }
}
