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

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.IWindowedSink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.typed.DirectCompute;
import edu.iu.dsc.tws.task.api.window.compute.WindowedCompute;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.Window;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public class STWindowExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STWindowExample.class.getName());

  private WindowingPolicy windowingPolicy;

  private Window window;

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    initPolicy();
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink d = new DirectReceiveTask();

    ISink dw = new DirectWindowedReceivingTask(this.windowingPolicy);

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, dw, sinkParallelism);
    computeConnection.direct(SOURCE, edge, DataType.INTEGER);

    return taskGraphBuilder;
  }

  public void initPolicy() {
    WindowConfig.Count count = new WindowConfig.Count(5);
    this.window = Window.TUMBLING;
    this.windowingPolicy = new WindowingPolicy(this.window, count);
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

  protected static class DirectWindowedReceivingTask extends WindowedCompute<int[]>
      implements IWindowedSink {

    public DirectWindowedReceivingTask(WindowingPolicy windowingPolicy) {
      super(windowingPolicy);
    }

    @Override
    public boolean window(List<int[]> content) {

      int[][] d = new int[content.size()][];
      for (int i = 0; i < d.length; i++) {
        d[i] = content.get(i);
      }

      LOG.info(String.format("Direct Data Window Received : %s ", Arrays.deepToString(d)));
      return true;
    }
  }
}
