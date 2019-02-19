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
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;

public class BTGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseSource g = new SourceBatchTask(edge);
    BaseSink r = new GatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.gather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class GatherSinkTask extends BaseSink {
    private static final long serialVersionUID = -254264903510284798L;
    private static int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        int numberOfElements = 0;
        int totalValues = 0;
        Iterator<Object> itr = (Iterator<Object>) message.getContent();
        while (itr.hasNext()) {
          Object data = itr.next();
          numberOfElements++;
          if (data instanceof Tuple) {
            experimentData.setOutput(((Tuple) data).getValue());
            try {
              verify(OperationNames.GATHER);
            } catch (VerificationException e) {
              LOG.info("Exception Message : " + e.getMessage());
            }
          }
          if (data instanceof int[]) {
            totalValues += ((int[]) data).length;
          }
        }

        /*if (count % jobParameters.getPrintInterval() == 0) {
          LOG.info("Gathered : " + message.getContent().getClass().getJobName()
              + " numberOfElements: " + numberOfElements
              + " total: " + totalValues);
        }*/

      }
      return true;
    }
  }
}
