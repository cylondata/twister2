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
package edu.iu.dsc.tws.examples.task.batch;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.typed.batch.BKeyedReduceCompute;

public class BTKeyedReduceExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTKeyedReduceExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelsim = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    Op operation = Op.SUM;
    DataType keyType = DataType.OBJECT;
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseSource g = new KeyedSourceBatchTask(edge);
    ISink r = new KeyedReduceSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelsim);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedReduce(SOURCE, edge, operation, keyType, dataType);
    return taskGraphBuilder;
  }

  protected static class KeyedReduceSinkTask
      extends BKeyedReduceCompute<Object, int[]> implements ISink {

    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean keyedReduce(Iterator<Tuple<Object, int[]>> content) {
      while (content.hasNext()) {
        Tuple<Object, int[]> value = content.next();
        Object key = value.getKey();
        Object val = value.getValue();
        if (count % jobParameters.getPrintInterval() == 0) {
          experimentData.setOutput(val);
          try {
            verify(OperationNames.KEYED_REDUCE);
          } catch (VerificationException e) {
            LOG.info("Exception Message : " + e.getMessage());
          }
        }
      }
      count++;
      return true;
    }
  }
}
