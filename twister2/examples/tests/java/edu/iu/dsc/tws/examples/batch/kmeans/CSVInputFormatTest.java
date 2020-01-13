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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.logging.Logger;

import org.junit.Test;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.examples.csv.KMeansDataGenerator;
import edu.iu.dsc.tws.task.graph.GraphBuilder;

public class CSVInputFormatTest {

  private static final Logger LOG = Logger.getLogger(CSVInputFormatTest.class.getName());

  private final Charset defaultCharset = StandardCharsets.UTF_8;

  /**
   * To test the CSV Input Format
   */
  @Test
  public void testUniqueSchedules() throws IOException {
    Config config = getConfig();
    Path path = new Path("/tmp/dinput/");
    createOutputFile(path, config);
    LocalCSVInputPartitioner csvInputPartitioner = new LocalCSVInputPartitioner(path, 4, config);
    csvInputPartitioner.configure(config);
    FileInputSplit[] inputSplits = csvInputPartitioner.createInputSplits(2);
    LOG.info("input split values are:" + Arrays.toString(inputSplits));
    InputSplitAssigner inputSplitAssigner = csvInputPartitioner.getInputSplitAssigner(inputSplits);
    InputSplit inputSplit = inputSplitAssigner.getNextInputSplit("localhost", 0);
    inputSplit.open(config);
    do {
      inputSplit.nextRecord(null);
    } while (!inputSplit.reachedEnd());
  }

  private void createOutputFile(Path path, Config config) throws IOException {
    KMeansDataGenerator.generateData("csv", path, 1, 100, 100, 2, config);
  }

  private ComputeGraph createBatchGraph(int parallel) {
    TestSource testSource = new TestSource();
    TestSink testSink = new TestSink();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", testSource);
    builder.setParallelism("source", parallel);
    builder.addTask("sink1", testSink);
    builder.setParallelism("sink1", parallel);
    builder.operationMode(OperationMode.BATCH);
    return builder.build();
  }

  private Config getConfig() {
    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT";
    String configDir = "/home/" + System.getProperty("user.dir")
        + "/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";
    Config config = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);
    return Config.newBuilder().putAll(config).build();
  }

  private WorkerPlan createWorkPlan(int workers) {
    WorkerPlan plan = new WorkerPlan();
    for (int i = 0; i < workers; i++) {
      Worker w = new Worker(i);
      w.addProperty("bandwidth", 1000.0);
      w.addProperty("latency", 0.1);
      plan.addWorker(w);
    }
    return plan;
  }

  /**
   * This is the test source class
   */
  public static class TestSource extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264903510284748L;

    private String inputKey;

    public TestSource() {
    }

    public TestSource(String inputkey) {
      this.inputKey = inputkey;
    }

    @Override
    public void execute() {
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public IONames getReceivableNames() {
      return IONames.declare(inputKey);
    }
  }

  public static class TestSink extends BaseCompute implements Collector {
    private static final long serialVersionUID = -254264903510284748L;

    private String inputKey;

    public TestSink() {
    }

    public TestSink(String inputkey) {
      this.inputKey = inputkey;
    }

    @Override
    public boolean execute(IMessage message) {
      return false;
    }

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public DataPartition<?> get(String name) {
      return null;
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare(inputKey);
    }
  }
}

