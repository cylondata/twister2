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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.task.dataparallelimpl.DataParallelConstants;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSink;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansJob extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(KMeansJob.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int workers = kMeansJobParameters.getWorkers();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();
    String outputDirecotry = kMeansJobParameters.getOutputDirectory();

    boolean shared = kMeansJobParameters.isShared();

    if (workerId == 0) {
      try {
        KMeansDataGenerator.generateData(
            "txt", new Path(dinputDirectory), numFiles, dsize, 100, dimension);
        KMeansDataGenerator.generateData(
            "txt", new Path(cinputDirectory), numFiles, csize, 100, dimension);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }

    /*TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    KMeansDataParallelSource sourceTask = new KMeansDataParallelSource();
    KMeansDataParallelSink sinkTask = new KMeansDataParallelSink();
    graphBuilder.addSource("map", sourceTask, parallelismValue);
    graphBuilder.addSink("reduce", sinkTask, parallelismValue);
    graphBuilder.setMode(OperationMode.BATCH);*/

    KMeansDataParallelSource sourceTask = new KMeansDataParallelSource();
    KMeansDataParallelSink sinkTask = new KMeansDataParallelSink();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection.partition("source", "reduce");
    graphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = graphBuilder.build();

    ExecutionPlan plan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, plan);
  }

  public static class KMeansDataParallelSource extends BaseSource {

    private static final Logger LOG = Logger.getLogger(KMeansDataParallelSource.class.getName());

    private static final long serialVersionUID = -1L;

    private DataSource<String, ?> source;

    private DataSink<String> sink;

    private String datainputDirectory;
    private String outputDirectory;

    private String centroidinputDirectory;
    private int numFiles;
    private int datapointSize;
    private int centroidSize;
    private int dimension;
    private int sizeOfMargin = 100;

    private Config config;
    private TaskContext taskContext;

    @Override
    public void execute() {
      LOG.info("Context Task Index:" + context.taskIndex());
      Map<Integer, String> partitionValue = new HashMap<>();
      InputSplit<String> inputSplit = source.getNextSplit(context.taskIndex());
      int splitCount = 0;
      int totalCount = 0;

      while (inputSplit != null) {
        try {
          StringBuilder stringBuilder = new StringBuilder();

          int count = 0;
          while (!inputSplit.reachedEnd()) {
            String value = inputSplit.nextRecord(null);
            //LOG.info("We read value: " + value);
            //sink.add(context.taskIndex(), value);
            if (value != null) {
              stringBuilder.append(value);
            }
            count += 1;
            totalCount += 1;
            partitionValue.put(context.taskIndex(), stringBuilder.toString());
          }
          splitCount += 1;
          inputSplit = source.getNextSplit(context.taskIndex());
          LOG.info("Finished: " + context.taskIndex() + " count: " + count
              + " split: " + splitCount + " total count: " + totalCount);
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      LOG.info("Partitioned Values:" + partitionValue.keySet()
          + "\t" + partitionValue.entrySet());
      context.writeEnd("partition", partitionValue);
      //sink.persist();
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      datainputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_DINPUT_DIRECTORY);
      centroidinputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_CINPUT_DIRECTORY);
      outputDirectory = cfg.getStringValue(DataParallelConstants.ARGS_OUTPUT_DIRECTORY);
      numFiles = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_NUMBER_OF_FILES));
      datapointSize = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_DSIZE));
      centroidSize = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_CSIZE));
      dimension = Integer.parseInt(cfg.getStringValue(DataParallelConstants.ARGS_DIMENSIONS));

      ExecutionRuntime runtime = (ExecutionRuntime)
          cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);

      LOG.info("Data Input Directory:" + datainputDirectory);

      boolean shared = cfg.getBooleanValue(DataParallelConstants.ARGS_SHARED_FILE_SYSTEM);
      if (!shared) {
        this.source = runtime.createInput(cfg, context,
            new LocalTextInputPartitioner(new Path(datainputDirectory), context.getParallelism()));

      } else {
        this.source = runtime.createInput(cfg, context,
            new SharedTextInputPartitioner(new Path(datainputDirectory)));
      }
      /*this.sink = new DataSink<>(cfg,
          new TextOutputWriter(FileSystem.WriteMode.OVERWRITE, new Path(outputDirectory)));

      LOG.info("This source is::::::::::::" + this.source + "\t" + this.sink);*/
    }
  }

  public static class KMeansDataParallelSink extends BaseSink {

    private static final Logger LOG = Logger.getLogger(KMeansDataParallelSink.class.getName());

    private static final long serialVersionUID = -1L;

    private DataSource<String, ?> source;
    private DataSink<String> sink;

    private String datainputDirectory;
    private String centroidinputDirectory;
    private String outputDirectory;
    private int numFiles;
    private int datapointSize;
    private int centroidSize;
    private int dimension;
    private int sizeOfMargin = 100;

    //private ArrayList<Iterator> partitionValue = new ArrayList<>(Arrays.asList());

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: (worker id) " + context.getWorkerId()
          + "\ttask id:" + context.taskId());
      LOG.info("map values:" + message.getContent());
      return true;
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

    /* public static class KMeansDataParallelSource extends DataParallelSourceTaskImpl {

      private static final Logger LOG = Logger.getLogger(KMeansDataParallelSource.class.getName());

      private static final long serialVersionUID = -1L;

      private DataSource<String, ?> source;

      private DataSink<String> sink;

      @Override
      public void prepare(Config cfg, TaskContext context) {
        super.prepare(cfg, context);
        LOG.fine("Context Task Index:" + context.taskIndex());
      }

      @Override
      public void execute() {
        super.execute();
        LOG.fine("Context Task Index:" + context.taskIndex());
        //DataParallelSourceTaskImpl dataParallelSourceTask = new DataParallelSourceTaskImpl();
        //dataParallelSourceTask.execute();
        context.writeEnd("reduce", "hello");
      }
    }

  public static class KMeansDataParallelSink extends DataParallelSinkTaskImpl {

    private static final Logger LOG = Logger.getLogger(KMeansDataParallelSink.class.getName());

    private static final long serialVersionUID = -1L;

    private DataSource<String, ?> source;

    private DataSink<String> sink;

    @Override
    public boolean execute(IMessage message) {
      super.execute(message);
      LOG.log(Level.INFO, "Received worker id: " + context.getWorkerId()
          + "\t" + message.getContent());
      return true;
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }*/
}
