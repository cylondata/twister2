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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskConfigurations;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class TaskGraphBuildTest {

  private static final Logger LOGGER = Logger.getLogger(TaskGraphBuildTest.class.getName());

  @Test
  public void testUniqueSchedules1() {
    DataFlowTaskGraph dataFlowTaskGraph = createGraph();
    Assert.assertNotNull(dataFlowTaskGraph);
    Assert.assertEquals(dataFlowTaskGraph.taskEdgeSet().iterator().next().getName(),
        TaskConfigurations.DEFAULT_EDGE);
    Assert.assertEquals(dataFlowTaskGraph.taskEdgeSet().size(), 2);
  }

  @Test
  public void testUniqueSchedules2() {
    DataFlowTaskGraph dataFlowTaskGraph = createGraph();

    Assert.assertEquals(dataFlowTaskGraph.getTaskVertexSet().iterator().next().getName(),
        TaskConfigurations.DEFAULT_EDGE);
    Assert.assertEquals(dataFlowTaskGraph.taskEdgeSet().size(), 2);
  }

  private DataFlowTaskGraph createGraph() {
    TestSource testSource = new TestSource();
    TestSink1 testCompute = new TestSink1();
    TestSink2 testSink = new TestSink2();

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(getConfig());

    taskGraphBuilder.addSource("source", testSource, 4);
    ComputeConnection computeConnection = taskGraphBuilder.addCompute(
        "compute", testCompute, 4);
    computeConnection.partition("source", TaskConfigurations.DEFAULT_EDGE, DataType.OBJECT);
    ComputeConnection rc = taskGraphBuilder.addSink("sink", testSink, 1);
    rc.allreduce("compute", TaskConfigurations.DEFAULT_EDGE, new Aggregator(), DataType.OBJECT);
    DataFlowTaskGraph graph = taskGraphBuilder.build();
    return graph;
  }

  public class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;
      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }
      return object31;
    }
  }

  private Config getConfig() {
    String twister2Home = "/home/kannan/twister2/bazel-bin/scripts/package/twister2-0.1.0";
    String configDir = "/home/kannan/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";
    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().putAll(config).build();
  }

  public static class TestSource extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public void execute() {
    }
  }

  public static class TestSink1 extends BaseCompute {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }

  public static class TestSink2 extends BaseSink {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }
}
