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
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;

public class BTPartitionExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTPartitionExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseSource g = new SourceBatchTask(edge);
    BaseSink r = new PartitionSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.partition(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {

  }

  protected static class PartitionSinkTask extends BaseSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {

      Object object = message.getContent();

      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          ((Iterator) message.getContent()).next();
          count++;
        }
        /*if (count % jobParameters.getPrintInterval() == 0) {
          Object object = message.getContent();
          experimentData.setOutput(object);
          try {
            verify(OperationNames.PARTITION);
          } catch (VerificationException e) {
            LOG.info("Exception Message : " + e.getMessage());
          }
        }*/
        if (count % jobParameters.getPrintInterval() == 0) {
          LOG.info("Received : " + object.getClass().getName());
          if (object instanceof Iterator<?>) {
            Iterator<?> itr = (Iterator<?>) object;
            LOG.info("INstance : " + itr.getClass().getName());
            LOG.info("ITr next : " + itr.hasNext());
            while (itr.hasNext()) {
              Object res = itr.next();
              LOG.info("Message Partition Received : " + res.getClass().getName()
                  + ", Count : " + count);
            }
          }
        }
      }

      return true;
    }
  }

}
