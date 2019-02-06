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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.cdfw.BaseDriver;
import edu.iu.dsc.tws.api.cdfw.CDFWExecutor;
import edu.iu.dsc.tws.api.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.api.cdfw.task.ConnectedSink;
import edu.iu.dsc.tws.api.cdfw.task.ConnectedSource;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.cdfw.CDFWWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public final class KMeansCDFWExample {
  private static final Logger LOG = Logger.getLogger(KMeansCDFWExample.class.getName());

  private KMeansCDFWExample() {
  }

  public static class KMeansDataFlowsDriver extends BaseDriver {
    @Override
    public void execute(Config config, CDFWExecutor exec) {
      // build JobConfig
      JobConfig jobConfig = new JobConfig();

      LOG.log(Level.INFO, "Executing the first graph");
      // run the first job
      runFirstJob(config, exec, 2, jobConfig);
      // run the second job
      LOG.log(Level.INFO, "Executing the second graph");
      runSecondJob(config, exec, 2, jobConfig);
    }
  }

  private static class FirstTask extends DataParallelSourceImpl {

    private static final long serialVersionUID = -254264120110286748L;

    protected FirstTask() {
    }

    @Override
    public void execute() {
      super.prepare(config, context);
      super.execute();
    }
  }

  private static class SecondTask extends DataParallelSinkImpl {

    private static final long serialVersionUID = -254264120110286748L;
    private String name;

    protected SecondTask() {
    }

    protected SecondTask(String outName) {
      this.name = outName;
    }

    public boolean execute(IMessage iMessage) {
      super.prepare(config, context);
      super.execute(iMessage);
      return true;
    }
  }

  private static class ThirdTask extends CentroidParallelSourceImpl {

    private static final long serialVersionUID = -254264120110286748L;
    private String name;

    protected ThirdTask() {
    }

    protected ThirdTask(String outName) {
      this.name = outName;
    }

    public void execute() {
      super.prepare(config, context);
      super.execute();
    }
  }

  private static class FirstSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd("partition", "Datapoints");
    }

    @Override
    public void add(String name, DataObject<?> data) {
      LOG.log(Level.FINE, "Received input: " + name);
    }
  }

  private static class SecondSourceTask extends ConnectedSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd("partition", "Centroids");
    }

    @Override
    public void add(String name, DataObject<?> data) {
      LOG.log(Level.FINE, "Received input: " + name);
    }
  }

  private static class ThirdSink extends ConnectedSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId() + message.getContent());
      return true;
    }

    @Override
    public DataPartition<Object> get() {
      return null;
    }
  }

  /**
   * This class aggregates the cluster centroid values and sum the new centroid values.
   */
  public static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    /**
     * The actual message callback
     *
     * @param object1 the actual message
     * @param object2 the actual message
     */
    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      return object1.toString() + object2.toString();
    }
  }

  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(KMeansConstants.ARGS_WORKERS, true, "Workers");
    options.addOption(KMeansConstants.ARGS_CSIZE, true, "Size of the dapoints file");
    options.addOption(KMeansConstants.ARGS_DSIZE, true, "Size of the centroids file");
    options.addOption(KMeansConstants.ARGS_NUMBER_OF_FILES, true, "Number of files");
    options.addOption(KMeansConstants.ARGS_SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(KMeansConstants.ARGS_DIMENSIONS, true, "dim");
    options.addOption(KMeansConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
    options.addOption(KMeansConstants.ARGS_NUMBER_OF_CLUSTERS, true, "clusters");

    options.addOption(Utils.createOption(KMeansConstants.ARGS_DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_FILE_SYSTEM,
        true, "file system", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_CSIZE));
    int numFiles = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_NUMBER_OF_FILES));
    int dimension = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        KMeansConstants.ARGS_PARALLELISM_VALUE));
    int numberOfClusters = Integer.parseInt(cmd.getOptionValue(
        KMeansConstants.ARGS_NUMBER_OF_CLUSTERS));

    String dataDirectory = cmd.getOptionValue(KMeansConstants.ARGS_DINPUT_DIRECTORY);
    String centroidDirectory = cmd.getOptionValue(KMeansConstants.ARGS_CINPUT_DIRECTORY);
    String outputDirectory = cmd.getOptionValue(KMeansConstants.ARGS_OUTPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(KMeansConstants.ARGS_FILE_SYSTEM);

    boolean shared =
        Boolean.parseBoolean(cmd.getOptionValue(KMeansConstants.ARGS_SHARED_FILE_SYSTEM));

    // we we are a shared file system, lets generate data at the client
    /*if (shared) {
      KMeansDataGenerator.generateData(
          "txt", new Path(dataDirectory), numFiles, dsize, 100, dimension);
      KMeansDataGenerator.generateData(
          "txt", new Path(centroidDirectory), numFiles, csize, 100, dimension);
    }*/

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(KMeansConstants.ARGS_DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(KMeansConstants.ARGS_CINPUT_DIRECTORY, centroidDirectory);
    jobConfig.put(KMeansConstants.ARGS_OUTPUT_DIRECTORY, outputDirectory);
    jobConfig.put(KMeansConstants.ARGS_FILE_SYSTEM, fileSystem);
    jobConfig.put(KMeansConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(KMeansConstants.ARGS_CSIZE, Integer.toString(csize));
    jobConfig.put(KMeansConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(KMeansConstants.ARGS_NUMBER_OF_FILES, Integer.toString(numFiles));
    jobConfig.put(KMeansConstants.ARGS_DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(KMeansConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(KMeansConstants.ARGS_SHARED_FILE_SYSTEM, shared);
    jobConfig.put(KMeansConstants.ARGS_NUMBER_OF_CLUSTERS, Integer.toString(numberOfClusters));

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    try {
      KMeansDataGenerator.generateData(
          "txt", new Path(dataDirectory), numFiles, dsize, 100, dimension);
      KMeansDataGenerator.generateData(
          "txt", new Path(centroidDirectory), numFiles, csize, 100, dimension);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create input data:", ioe);
    }


    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(KMeansCDFWExample.class.getName())
        .setWorkerClass(CDFWWorker.class)
        .setDriverClass(KMeansDataFlowsDriver.class.getName())
        .addComputeResource(1, 512, workers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  private static void runFirstJob(Config config, CDFWExecutor cdfwExecutor,
                                  int parallelismValue, JobConfig jobConfig) {

    /*KMeansDataParallelWorker.KMeansDataParallelSource firstSourceTask
        = new KMeansDataParallelWorker.KMeansDataParallelSource();
    KMeansDataParallelWorker.KMeansDataParallelSink connectedSink
        = new KMeansDataParallelWorker.KMeansDataParallelSink("first_out");*/

    FirstTask firstSourceTask = new FirstTask();
    SecondTask connectedSink = new SecondTask();

    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source", firstSourceTask, parallelismValue);
    ComputeConnection computeConnection = graphBuilderX.addSink("sink", connectedSink,
        parallelismValue);
    computeConnection.direct("source", "direct", DataType.OBJECT);
    graphBuilderX.setMode(OperationMode.BATCH);

    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first_graph", batchGraph).
        setWorkers(parallelismValue).addJobConfig(jobConfig);
    cdfwExecutor.execute(job);
  }

  private static void runSecondJob(Config config, CDFWExecutor cdfwExecutor,
                                   int parallelismValue, JobConfig jobConfig) {

    ThirdTask connectedSource = new ThirdTask();
    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", connectedSource, parallelismValue);

    graphBuilderX.setMode(OperationMode.BATCH);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("second_graph", batchGraph).
        setWorkers(parallelismValue).addJobConfig(jobConfig);
    cdfwExecutor.execute(job);
  }

  /*private static void runThirdJob(Config config, CDFWExecutor cdfwExecutor,
                                   int parallelismValue, JobConfig jobConfig) {
    ConnectedSource connectedSource = new ConnectedSource("reduce");
    ConnectedSink connectedSink = new ConnectedSink();

    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", connectedSource, parallelismValue);
    ComputeConnection reduceConn = graphBuilderX.addSink("sink1", connectedSink,
        1);
    reduceConn.reduce("source1", "reduce", new Aggregator(),
        DataType.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first_graph", batchGraph).
        setWorkers(parallelismValue).addJobConfig(jobConfig).addOutput("first_out");
    cdfwExecutor.execute(job);
  }*/
}
