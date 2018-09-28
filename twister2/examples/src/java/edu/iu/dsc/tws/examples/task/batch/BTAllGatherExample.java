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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;

public class BTAllGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(BTAllGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int psource = taskStages.get(0);
    int psink = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new AllGatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.allgather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  protected static class AllGatherSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public boolean execute(IMessage message) {
      Object object = message.getContent();
      if (object instanceof int[]) {
        LOG.info("Batch AllGather Message Received : " + Arrays.toString((int[]) object));
      } else if (object instanceof Iterator) {
        Iterator<?> it = (Iterator<?>) object;
        int[] a = {};
        ArrayList<int[]> data = new ArrayList<>();
        while (it.hasNext()) {
          if (it.next() instanceof int[]) {
            a = (int[]) it.next();
            LOG.info("Data : " + Arrays.toString(a));
            data.add(a);
          }
        }
        LOG.info("Batch AllGather Task Message Receieved : " + data.size());

      } else {
        LOG.info("Class : " + object.getClass().getName());
      }
      return true;
    }
  }
}
