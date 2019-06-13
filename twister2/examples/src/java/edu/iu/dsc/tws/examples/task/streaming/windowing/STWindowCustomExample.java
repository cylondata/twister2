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
package edu.iu.dsc.tws.examples.task.streaming.windowing;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.BaseWindowSource;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.config.SlidingCountWindow;
import edu.iu.dsc.tws.task.api.window.config.SlidingDurationWindow;
import edu.iu.dsc.tws.task.api.window.config.TumblingCountWindow;
import edu.iu.dsc.tws.task.api.window.config.TumblingDurationWindow;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.core.BaseWindowedSink;

public class STWindowCustomExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STWindowCustomExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseWindowSource g = new SourceWindowTask(edge);

    BaseWindowedSink dw = new DirectCustomWindowReceiver()
        .withWindow(TumblingCountWindow.of(5));
    BaseWindowedSink dwDuration = new DirectCustomWindowReceiver()
        .withWindow(TumblingDurationWindow.of(2));

    BaseWindowedSink sdw = new DirectCustomWindowReceiver()
        .withWindow(SlidingCountWindow.of(5, 2));

    WindowConfig.Duration windowLength = new WindowConfig.Duration(2, TimeUnit.MILLISECONDS);
    WindowConfig.Duration slidingLength = new WindowConfig.Duration(2, TimeUnit.MILLISECONDS);

    BaseWindowedSink sdwDuration = new DirectCustomWindowReceiver()
        .withWindow(SlidingDurationWindow.of(windowLength, slidingLength));

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, sdwDuration, sinkParallelism);
    computeConnection.direct(SOURCE)
        .viaEdge(edge)
        .withDataType(DataType.INTEGER);

    return taskGraphBuilder;
  }

  protected static class DirectCustomWindowReceiver extends BaseWindowedSink<int[]> {

    public DirectCustomWindowReceiver() {
    }

    @Override
    public boolean execute(IWindowMessage<int[]> windowMessage) {
      LOG.info(String.format("Items : %d ", windowMessage.getWindow().size()));
      return true;
    }

    @Override
    public boolean getExpire(IWindowMessage<int[]> expiredMessages) {
      return true;
    }

    @Override
    public boolean getLateMessages(IMessage<int[]> lateMessage) {
      LOG.info(String.format("Late Message : %s",
          lateMessage.getContent() != null ? Arrays.toString(lateMessage.getContent()) : "null"));
      return true;
    }
  }
}
