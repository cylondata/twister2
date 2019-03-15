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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.typed.batch.BBroadCastCompute;

public class BTBroadCastExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTBroadCastExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    String edge = "edge";
    BaseSource g = new SourceBatchTask(edge);
    ISink r = new BroadcastSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.broadcast(SOURCE, edge);
    return taskGraphBuilder;
  }

  protected static class BroadcastSinkTask extends BBroadCastCompute<int[]> implements ISink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean broadcast(Iterator<Tuple<Integer, int[]>> content) {

      while (content.hasNext()) {
        experimentData.setOutput(content.next());
        LOG.log(Level.INFO, String.format("Received messages to %d: %d",
            context.taskId(), count));
        try {
          verify(OperationNames.BROADCAST);
        } catch (VerificationException e) {
          LOG.info("Exception Message : " + e.getMessage());
        }
      }
      return true;
    }
  }
}
