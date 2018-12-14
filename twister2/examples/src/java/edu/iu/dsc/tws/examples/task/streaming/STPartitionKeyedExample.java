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
package edu.iu.dsc.tws.examples.task.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;

public class STPartitionKeyedExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STPartitionKeyedExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.INTEGER;
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseSource g = new KeyedSourceStreamTask(edge);
    BaseSink r = new SKeyedPartitionSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedPartition(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }

  protected static class SKeyedPartitionSinkTask extends BaseSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      Object object = message.getContent();
      if (object instanceof ArrayList) {
        ArrayList<?> data = (ArrayList<?>) object;
        for (int i = 0; i < data.size(); i++) {
          Object value = data.get(i);
          experimentData.setOutput(value);
          try {
            verify(OperationNames.KEYED_PARTITION);
          } catch (VerificationException e) {
            LOG.info("Exception Message : " + e.getMessage());
          }
        }
      }
     /* if (count % jobParameters.getPrintInterval() == 0) {
        LOG.info(String.format("%d %d Streaming Message Keyed Partition Received count: %d",
            context.getWorkerId(),
            context.taskId(), count));
      }*/
      count++;
      return true;
    }
  }

}
