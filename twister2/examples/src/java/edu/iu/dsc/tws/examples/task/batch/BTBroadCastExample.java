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

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;

public class BTBroadCastExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTBroadCastExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new BroadcastSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.broadcast(SOURCE, edge);
    return taskGraphBuilder;
  }

  protected static class BroadcastSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private static int count = 0;

    @Override
    public boolean execute(IMessage message) {
      Object object = message.getContent();
      if (object instanceof int[]) {
        if (count % jobParameters.getPrintInterval() == 0) {
          LOG.info(" Batch Message Broadcasted : "
              + Arrays.toString((int[]) object) + ", counter : " + count);
        }
        count++;
      }
      return true;
    }
  }
}
