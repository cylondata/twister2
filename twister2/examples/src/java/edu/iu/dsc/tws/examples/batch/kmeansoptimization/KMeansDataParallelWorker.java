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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.cdfw.task.ConnectedSink;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.api.out.TextOutputWriter;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataSink;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansDataParallelWorker extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(KMeansJob.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    boolean shared = kMeansJobParameters.isShared();

    if (workerId == 0) {
      try {
        KMeansDataGenerator.generateData(
            "txt", new Path(dinputDirectory), numFiles, dsize, 100, dimension);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    KMeansDataParallelSource sourceTask = new KMeansDataParallelSource();
    KMeansDataParallelSink sinkTask = new KMeansDataParallelSink("out");

    taskGraphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection.direct("source", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, plan);
  }

  public static class KMeansDataParallelSource extends BaseSource {

    private static final Logger LOG = Logger.getLogger(KMeansDataParallelSource.class.getName());

    private static final long serialVersionUID = -1L;

    private DataSource<String, ?> source;
    private DataSink<String> sink; //TODO:remove the sink object

    @Override
    public void execute() {
      LOG.info("Context Task Index:" + context.taskIndex());
      InputSplit<String> inputSplit = source.getNextSplit(context.taskIndex());
      int splitCount = 0;
      int totalCount = 0;
      while (inputSplit != null) {
        try {
          int count = 0;
          while (!inputSplit.reachedEnd()) {
            String value = inputSplit.nextRecord(null); //Bug here...
            if (value != null) {
              LOG.info("We read value: " + value); //First index reads more value than for
              //example 100,index 0 reads 51 & 1 reads 49.
              sink.add(context.taskIndex(), value);
              context.write("direct", value);
              count += 1;
              totalCount += 1;
            }
          }
          LOG.info("************************************************************************");
          splitCount += 1;
          inputSplit = source.getNextSplit(context.taskIndex());
          LOG.info("Task index:" + context.taskIndex() + " count: " + count
              + "split: " + splitCount + " total count: " + totalCount);
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      sink.persist();
      context.end("direct");
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);

      String datainputDirectory = cfg.getStringValue(KMeansConstants.ARGS_DINPUT_DIRECTORY);
      ExecutionRuntime runtime = (ExecutionRuntime)
          cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
      String outDir = cfg.getStringValue(KMeansConstants.ARGS_OUTPUT_DIRECTORY);

      boolean shared = cfg.getBooleanValue(KMeansConstants.ARGS_SHARED_FILE_SYSTEM);
      if (!shared) {
        this.source = runtime.createInput(cfg, context,
            new LocalTextInputPartitioner(new Path(datainputDirectory), context.getParallelism()));
      } else {
        this.source = runtime.createInput(cfg, context,
            new SharedTextInputPartitioner(new Path(datainputDirectory)));
      }
      this.sink = new DataSink<String>(cfg,
          new TextOutputWriter(FileSystem.WriteMode.OVERWRITE, new Path(outDir)));
    }
  }

  public static class KMeansDataParallelSink extends ConnectedSink {

    private static final Logger LOG = Logger.getLogger(KMeansDataParallelSink.class.getName());

    private static final long serialVersionUID = -1L;

    private int dsize;
    private int dimension;
    private double[][] datapoint;
    // make the dynamic value for the row
    private DataObject<double[][]> datapoints = null;
    private String name;

    public KMeansDataParallelSink(String outName) {
      this.name = outName;
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "worker id " + context.getWorkerId()
          + "\ttask id:" + context.taskIndex());
      Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>) message.getContent();
      datapoint = new double[dsize + 1][dimension]; //Remove +1 after fixing the bug...
      int value = 0;
      while (arrayListIterator.hasNext()) {
        String val = String.valueOf(arrayListIterator.next());
        String[] data = val.split(",");
        for (int i = 0; i < dimension; i++) {
          datapoint[value][i] = Double.parseDouble(data[i].trim());
        }
        value++;
      }
      LOG.info("Dpoints::::" + Arrays.deepToString(datapoint)); //for testing
      datapoints.addPartition(new EntityPartition<>(0, datapoint));
      //context.writeEnd("partition", datapoints);
      return true;
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      dimension = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));
      dsize = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DSIZE))
          / context.getParallelism();
      datapoints = new DataObjectImpl<>(config);
    }
  }
}
