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
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public class STGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int psource = taskStages.get(0);
    int psink = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseStreamSource g = new SourceStreamTask(edge);
    BaseStreamSink r = new GatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.gather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }

  protected static class GatherSinkTask extends BaseStreamSink {
    private int count = 0;
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public boolean execute(IMessage message) {
      if (count % 100 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Stream Message Gathered : " + Arrays.toString((int[]) object)
              + ", Count : " + count);
        } else if (object instanceof ArrayList) {
          ArrayList<?> a = (ArrayList<?>) object;
          String out = "";
          for (int i = 0; i < a.size(); i++) {
            Object o = a.get(i);
            if (o instanceof int[]) {
              out += Arrays.toString((int[]) o);
            }
          }
          LOG.info("Stream Message Gathered : " + out + ", Count : " + count);
        } else {
          LOG.info("Stream Message Gathered : " + message.getContent().getClass().getName()
              + ", Count : " + count);
        }

      }
      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
      }
      return true;
    }
  }
}
