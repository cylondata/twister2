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
package edu.iu.dsc.tws.examples.batch.cdfw;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectCompute;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.task.cdfw.DafaFlowJobConfig;
import edu.iu.dsc.tws.task.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;

public final class ConnectedDataflowExample {
  private static final Logger LOG = Logger.getLogger(ConnectedDataflowExample.class.getName());

  private ConnectedDataflowExample() {
  }

  public static class ConnectedDataflowExampleDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv cdfwEnv) {

      Config config = cdfwEnv.getConfig();

      DafaFlowJobConfig jobConfig = new DafaFlowJobConfig();

      DataFlowGraph job1 = generateFirstJob(config, 4, jobConfig);
      DataFlowGraph job2 = generateSecondJob(config, 4, jobConfig);
      DataFlowGraph job3 = generateThirdJob(config, 4, jobConfig);

      //todo: CDFWExecutor.executeCDFW(DataFlowGraph... graph) deprecated

      //cdfwEnv.executeDataFlowGraph(job1);
      //cdfwEnv.executeDataFlowGraph(job2);

      cdfwEnv.executeDataFlowGraph(job1, job2);
      //cdfwEnv.executeDataFlowGraph(job3);
    }
  }

  private static class FirstSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private String edgeName;

    private FirstSourceTask() {
    }

    protected FirstSourceTask(String edgename) {
      this.edgeName = edgename;
    }

    @Override
    public void execute() {
      LOG.info("edge name is:" + edgeName);
      context.writeEnd(edgeName, "firstsource");
    }
  }

  private static class FirstSinkTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -254264120110286748L;

    private String inputKey;
    private String edgeName;

    protected FirstSinkTask(String edgename, String inputkey) {
      this.edgeName = edgename;
      this.inputKey = inputkey;
    }

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
    }

    @Override
    public boolean execute(IMessage content) {
      context.writeEnd(edgeName, "firstout");
      return true;
    }
  }

  private static class SecondSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private String edgeName;

    private SecondSourceTask() {
    }

    protected SecondSourceTask(String edgename) {
      this.edgeName = edgename;
    }

    @Override
    public void execute() {
      context.writeEnd(edgeName, "secondsource");
    }
  }

  private static class SecondSinkTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -254264120110286748L;

    private String inputKey;
    private String edgeName;

    private SecondSinkTask() {
    }

    protected SecondSinkTask(String edgename, String inputkey) {
      this.edgeName = edgename;
      this.inputKey = inputkey;
    }

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
    }

    @Override
    public boolean execute(IMessage content) {
      context.writeEnd(edgeName, "secondout");
      return true;
    }
  }

  private static class ThirdSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private String edgeName;

    private ThirdSourceTask() {
    }

    protected ThirdSourceTask(String edgename) {
      this.edgeName = edgename;
    }

    @Override
    public void execute() {
      context.writeEnd(edgeName, "thirdsource");
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public Set<String> getReceivableNames() {
      Set<String> inputKeys = new HashSet<>();
      inputKeys.add("points");
      inputKeys.add("centroids");
      return inputKeys;
    }
  }

  private static class ThirdSinkTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -254264120110286748L;

    private String inputKey;
    private String edgeName;

    private ThirdSinkTask() {
    }

    protected ThirdSinkTask(String edgename, String inputkey) {
      this.edgeName = edgename;
      this.inputKey = inputkey;
    }

    @Override
    public DataPartition<?> get() {
      return null;
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
    }

    @Override
    public boolean execute(IMessage content) {
      context.writeEnd(edgeName, "Hello");
      return true;
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

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    Options options = new Options();
    options.addOption(CDFConstants.ARGS_PARALLELISM_VALUE, true, "2");
    options.addOption(CDFConstants.ARGS_WORKERS, true, "2");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int instances = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));

    configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(instances));
    configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(ParallelDataFlowsExample.class.getName())
        .setWorkerClass(CDFWWorker.class)
        .setDriverClass(ConnectedDataflowExampleDriver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }


  private static DataFlowGraph generateFirstJob(Config config, int parallelismValue,
                                                DafaFlowJobConfig jobConfig) {

    /*FirstSourceTask firstSourceTask = new FirstSourceTask(Context.TWISTER2_DIRECT_EDGE + "1");
    FirstSinkTask firstSinkTask = new FirstSinkTask(Context.TWISTER2_DIRECT_EDGE + "1", "points");

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    firstGraphBuilder.addSource("pointsource", firstSourceTask, parallelismValue);
    ComputeConnection firstComputeConnection = firstGraphBuilder.addSink("pointsink",
        firstSinkTask, parallelismValue);

    firstComputeConnection.partition("pointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    firstGraphBuilder.setMode(OperationMode.BATCH);
    ComputeGraph firstGraph = firstGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first_graph", firstGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig).addOutput("first_out");
    return job;
*/

    String dataDirectory = "/tmp/dinput0";
    int dsize = 1000;
    int dimension = 2;

    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink("points");
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsComputeGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsComputeGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);

    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
    ComputeGraph firstGraph = datapointsComputeGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first_graph", firstGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig).addOutput("first_out");
    return job;
  }

  private static DataFlowGraph generateSecondJob(Config config, int parallelismValue,
                                                 DafaFlowJobConfig jobConfig) {

    /*SecondSourceTask secondSourceTask = new SecondSourceTask(Context.TWISTER2_DIRECT_EDGE + "2");
    SecondSinkTask secondSinkTask = new SecondSinkTask(Context.TWISTER2_DIRECT_EDGE + "2",
        "centroids");

    ComputeGraphBuilder secondGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    secondGraphBuilder.addSource("centroidsource", secondSourceTask, parallelismValue);
    ComputeConnection secondComputeConnection = secondGraphBuilder.addSink("centroidsink",
        secondSinkTask, parallelismValue);

    secondComputeConnection.partition("centroidsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE + "2")
        .withDataType(MessageTypes.OBJECT);

    secondGraphBuilder.setMode(OperationMode.BATCH);
    ComputeGraph secondGraph = secondGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("second_graph", secondGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig).addOutput("second_out");
    return job;*/

    String centroidDirectory = "/tmp/cinput0";
    int csize = 1000;
    int dimension = 2;

    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
    KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, csize, dimension);
    KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink("centroids");
    ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    centroidsComputeGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource,
        parallelismValue);
    ComputeConnection centroidComputeConnection = centroidsComputeGraphBuilder.addCompute(
        "centroidcompute", centroidObjectCompute, parallelismValue);
    ComputeConnection secondGraphComputeConnection = centroidsComputeGraphBuilder.addSink(
        "centroidsink", centroidObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    centroidComputeConnection.direct("centroidsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphComputeConnection.direct("centroidcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    centroidsComputeGraphBuilder.setMode(OperationMode.BATCH);
    centroidsComputeGraphBuilder.setTaskGraphName("centTG");

    //Build the second taskgraph
    ComputeGraph secondGraph = centroidsComputeGraphBuilder.build();
    DataFlowGraph job = DataFlowGraph.newSubGraphJob("second_graph", secondGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig).addOutput("second_out");
    return job;
  }


  private static DataFlowGraph generateThirdJob(Config config, int parallelismValue,
                                                DafaFlowJobConfig jobConfig) {
   /* ThirdSourceTask thirdSourceTask = new ThirdSourceTask(Context.TWISTER2_DIRECT_EDGE + "3");
    ThirdSinkTask thirdSinkTask = new ThirdSinkTask(Context.TWISTER2_DIRECT_EDGE + "3",
        "centroids");

    ComputeGraphBuilder thirdGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    thirdGraphBuilder.addSource("kmeanssource", thirdSourceTask, parallelismValue);
    ComputeConnection thirdComputeConnection = thirdGraphBuilder.addSink("kmeanssink",
        thirdSinkTask, parallelismValue);

    thirdComputeConnection.direct("kmeanssource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE + "3")
        .withDataType(MessageTypes.OBJECT);

    thirdGraphBuilder.setMode(OperationMode.BATCH);
    ComputeGraph thirdGraph = thirdGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("third_graph", thirdGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig)
        .addInput("first_graph", "first_out")
        .addInput("second_graph", "second_out");
    return job;*/

    KMeansWorker.KMeansSourceTask kMeansSourceTask = new KMeansWorker.KMeansSourceTask();
    KMeansWorker.KMeansAllReduceTask kMeansAllReduceTask = new KMeansWorker.KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new KMeansWorker.CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
    kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
    ComputeGraph thirdGraph =  kmeansComputeGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("third_graph", thirdGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig)
        .addInput("first_graph", "first_out")
        .addInput("second_graph", "second_out");
    return job;
  }
}
