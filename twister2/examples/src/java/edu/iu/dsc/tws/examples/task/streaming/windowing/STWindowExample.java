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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.typed.DirectCompute;
import edu.iu.dsc.tws.task.api.window.BaseWindowSource;
import edu.iu.dsc.tws.task.api.window.api.BaseWindowSink;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.WindowType;
import edu.iu.dsc.tws.task.api.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public class STWindowExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STWindowExample.class.getName());

  private WindowType windowType;

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseWindowSource g = new SourceWindowTask(edge);
    ISink d = new DirectReceiveTask();

    WindowConfig.Count count1 = new WindowConfig.Count(10);
    WindowType windowType1 = WindowType.TUMBLING;
    WindowConfig.Count count2 = new WindowConfig.Count(3);
    WindowType windowType2 = WindowType.TUMBLING;
    WindowConfig.Duration duration1 = new WindowConfig.Duration(10, TimeUnit.MINUTES);
    WindowType windowType3 = WindowType.TUMBLING;

    // Adding multiple policies
    BaseWindowSink dw = new DirectWindowedReceivingTask()
        .withWindowCount(windowType1, count1);

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, dw, sinkParallelism);
    computeConnection.direct(SOURCE, edge, DataType.INTEGER);

    return taskGraphBuilder;
  }



  protected static class DirectReceiveTask extends DirectCompute<int[]> implements ISink {
    private static final long serialVersionUID = -254264903510284798L;

    private int count = 0;


    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }

    @Override
    public boolean direct(int[] content) {
      LOG.info(String.format("Direct Data Received : %s ", Arrays.toString(content)));
      return true;
    }
  }

  protected static class DirectWindowedReceivingTask extends BaseWindowedSink<int[]> {

    private WindowingPolicy windowingPolicy;

    public DirectWindowedReceivingTask() {
    }

    public DirectWindowedReceivingTask(WindowingPolicy windowingPolicy) {
      super(windowingPolicy);
      this.windowingPolicy = windowingPolicy;
    }


    /**
     * This method returns the final windowing message
     * @param windowMessage Aggregated IWindowMessage is obtained here
     * windowMessage contains [expired-tuples, current-tuples]
     * @return
     */
    @Override
    public IWindowMessage<int[]> execute(IWindowMessage<int[]> windowMessage) {
      LOG.info(String.format("Items : %d ", windowMessage.getWindow().size()));
      return windowMessage;
    }


  }
}
