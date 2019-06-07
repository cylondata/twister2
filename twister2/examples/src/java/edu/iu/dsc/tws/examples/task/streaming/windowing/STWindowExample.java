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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.typed.DirectCompute;
import edu.iu.dsc.tws.task.api.window.BaseWindowSource;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.collectives.AggregateWindow;
import edu.iu.dsc.tws.task.api.window.collectives.FoldWindow;
import edu.iu.dsc.tws.task.api.window.collectives.ProcessWindow;
import edu.iu.dsc.tws.task.api.window.collectives.ReduceWindow;
import edu.iu.dsc.tws.task.api.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.api.window.function.AggregateWindowedFunction;
import edu.iu.dsc.tws.task.api.window.function.FoldWindowedFunction;
import edu.iu.dsc.tws.task.api.window.function.ProcessWindowedFunction;
import edu.iu.dsc.tws.task.api.window.function.ReduceWindowedFunction;

public class STWindowExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STWindowExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseWindowSource g = new SourceWindowTask(edge);

    // Tumbling Window
    BaseWindowedSink dw = new DirectWindowedReceivingTask()
        .withTumblingCountWindow(5);
    BaseWindowedSink dwDuration = new DirectWindowedReceivingTask()
        .withTumblingDurationWindow(2, TimeUnit.MILLISECONDS);

    // Sliding Window
    BaseWindowedSink sdw = new DirectWindowedReceivingTask()
        .withSlidingCountWindow(5, 2);

    BaseWindowedSink sdwDuration = new DirectWindowedReceivingTask()
        .withSlidingDurationWindow(2, TimeUnit.MILLISECONDS,
            1, TimeUnit.MILLISECONDS);

    BaseWindowedSink sdwDurationReduce = new DirectReduceWindowedTask(new ReduceFunctionImpl())
        .withSlidingDurationWindow(2, TimeUnit.MILLISECONDS,
            1, TimeUnit.MILLISECONDS);

    BaseWindowedSink sdwCountSlidingReduce = new DirectReduceWindowedTask(new ReduceFunctionImpl())
        .withSlidingCountWindow(5, 2);

    BaseWindowedSink sdwCountTumblingReduce = new DirectReduceWindowedTask(new ReduceFunctionImpl())
        .withTumblingCountWindow(5);

    BaseWindowedSink sdwCountTumblingAggregate
        = new DirectAggregateWindowedTask(new AggregateFunctionImpl(1, 2))
        .withTumblingCountWindow(5);

    BaseWindowedSink sdwCountTumblingFold = new DirectFoldWindowedTask(new FoldFunctionImpl())
        .withTumblingCountWindow(5);

    BaseWindowedSink sdwCountTumblingProcess
        = new DirectProcessWindowedTask(new ProcessFunctionImpl())
        .withSlidingDurationWindow(5, TimeUnit.MILLISECONDS, 3,
            TimeUnit.MILLISECONDS);


    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, sdwCountTumblingProcess, sinkParallelism);
    computeConnection.direct(SOURCE, edge, DataType.INTEGER_ARRAY);

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

    public DirectWindowedReceivingTask() {
    }

    /**
     * This method returns the final windowing message
     *
     * @param windowMessage Aggregated IWindowMessage is obtained here
     * windowMessage contains [expired-tuples, current-tuples]
     */
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

  protected static class DirectReduceWindowedTask extends ReduceWindow<int[]> {


    public DirectReduceWindowedTask(ReduceWindowedFunction<int[]> reduceWindowedFunction) {
      super(reduceWindowedFunction);
    }

    @Override
    public boolean reduce(int[] content) {
      LOG.info("Window Reduced Value : " + Arrays.toString(content));
      return true;
    }

    @Override
    public boolean reduceLateMessage(int[] content) {
      LOG.info(String.format("Late Reduced Message : %s", Arrays.toString(content)));
      return false;
    }
  }

  protected static class DirectAggregateWindowedTask extends AggregateWindow<int[]> {

    public DirectAggregateWindowedTask(AggregateWindowedFunction aggregateWindowedFunction) {
      super(aggregateWindowedFunction);
    }

    @Override
    public boolean aggregate(int[] message) {
      LOG.info("Window Aggregate Value : " + Arrays.toString(message));
      return true;
    }

    @Override
    public boolean aggregateLateMessages(int[] message) {
      LOG.info(String.format("Late Aggregate Message : %s", Arrays.toString(message)));
      return true;
    }
  }

  protected static class DirectFoldWindowedTask extends FoldWindow<int[], String> {

    public DirectFoldWindowedTask(FoldWindowedFunction<int[], String> foldWindowedFunction) {
      super(foldWindowedFunction);
    }

    @Override
    public boolean fold(String content) {
      LOG.info("Window Fold Value : " + content);
      return true;
    }

    @Override
    public boolean foldLateMessage(String lateMessage) {
      LOG.info(String.format("Late Aggregate Message : %s", lateMessage));
      return false;
    }
  }

  protected static class DirectProcessWindowedTask extends ProcessWindow<int[]> {

    public DirectProcessWindowedTask(ProcessWindowedFunction<int[]> processWindowedFunction) {
      super(processWindowedFunction);
    }

    @Override
    public boolean process(IWindowMessage<int[]> windowMessage) {
      for (IMessage<int[]> msg : windowMessage.getWindow()) {
        int[] msgC = msg.getContent();
        LOG.info("Process Window Value : " + Arrays.toString(msgC));
      }
      return true;
    }

    @Override
    public boolean processLateMessages(IMessage<int[]> lateMessage) {
      LOG.info(String.format("Late Message : %s",
          lateMessage.getContent() != null ? Arrays.toString(lateMessage.getContent()) : "null"));
      return true;
    }
  }


  protected static class ReduceFunctionImpl implements ReduceWindowedFunction<int[]> {

    @Override
    public int[] onMessage(int[] object1, int[] object2) {
      int[] ans = new int[object1.length];
      for (int i = 0; i < object1.length; i++) {
        ans[i] = object1[i] + object2[i];
      }
      return ans;
    }

    @Override
    public int[] reduceLateMessage(int[] lateMessage) {
      for (int i = 0; i < lateMessage.length; i++) {
        lateMessage[i] = lateMessage[i] * 2;
      }
      return lateMessage;
    }
  }

  protected static class AggregateFunctionImpl implements AggregateWindowedFunction<int[]> {

    private int weight1;

    private int weight2;

    public AggregateFunctionImpl(int weight1, int weight2) {
      this.weight1 = weight1;
      this.weight2 = weight2;
    }

    @Override
    public int[] onMessage(int[] object1, int[] object2) {
      int[] ans = new int[object1.length];
      for (int i = 0; i < object1.length; i++) {
        ans[i] = this.weight1 * object1[i] + this.weight2 * object2[i];
      }
      return ans;
    }
  }

  protected static class FoldFunctionImpl implements FoldWindowedFunction<int[], String> {

    private int[] ans;

    @Override
    public String computeFold() {
      String summary = "Window Value With Basic Per Window Averaging : "
          + Arrays.toString(this.ans);
      return summary;
    }

    @Override
    public int[] onMessage(int[] object1, int[] object2) {
      this.ans = new int[object1.length];
      for (int i = 0; i < object1.length; i++) {
        ans[i] = (object1[i] + object2[i]) / 2;
      }
      return ans;
    }
  }

  protected static class ProcessFunctionImpl implements ProcessWindowedFunction<int[]> {

    @Override
    public IWindowMessage<int[]> process(IWindowMessage<int[]> windowMessage) {
      int[] current = null;
      List<IMessage<int[]>> messages = new ArrayList<>(windowMessage.getWindow().size());
      for (IMessage<int[]> msg : windowMessage.getWindow()) {
        int[] value = msg.getContent();
        if (current == null) {
          current = value;
        } else {
          current = add(current, value);
          messages.add(new TaskMessage<>(current));
        }

      }
      WindowMessageImpl<int[]> windowMessage1 = new WindowMessageImpl<>(messages);
      return windowMessage1;
    }

    @Override
    public IMessage<int[]> processLateMessage(IMessage<int[]> lateMessage) {
      int[] res = lateMessage.getContent();
      if (res != null) {
        for (int i = 0; i < res.length; i++) {
          res[i] = res[i];
        }
      }
      return new TaskMessage<>(res);
    }

    @Override
    public int[] onMessage(int[] object1, int[] object2) {
      if (object1 != null && object2 != null) {
        return add(object1, object2);
      }
      return null;
    }

    private int[] add(int[] a1, int[] a2) {
      int[] ans = new int[a1.length];
      for (int i = 0; i < a1.length; i++) {
        ans[i] = a1[i] + a2[i];
      }
      return ans;
    }
  }
}
