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
package edu.iu.dsc.tws.examples.internal.batchscheduler;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectCompute;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class BatchTaskSchedulerExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(BatchTaskSchedulerExample.class.getName());

  private static int parallelismValue;
  private static int workers;

  public static ComputeGraph buildFirstGraph(String dataDirectory, int dsize,
                                             int parallelism, int dimension,
                                             Config conf) {
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelism, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink("points");
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", dataObjectSource,
        parallelism);
    ComputeConnection datapointComputeConnection = datapointsComputeGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelism);
    ComputeConnection firstGraphComputeConnection = datapointsComputeGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelism);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);

    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
    //Build the first taskgraph
    return datapointsComputeGraphBuilder.build();
  }

  public static ComputeGraph buildSecondGraph(String centroidDirectory, int csize,
                                              int parallelism, int dimension,
                                              Config conf) {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
    KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, csize, dimension);
    KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink("centroids");
    ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    centroidsComputeGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource,
        parallelism);
    ComputeConnection centroidComputeConnection = centroidsComputeGraphBuilder.addCompute(
        "centroidcompute", centroidObjectCompute, parallelism);
    ComputeConnection secondGraphComputeConnection = centroidsComputeGraphBuilder.addSink(
        "centroidsink", centroidObjectSink, parallelism);

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
    return centroidsComputeGraphBuilder.build();
  }

  public static ComputeGraph buildThirdGraph(int parallelism, Config conf) {
    KMeansWorker.KMeansSourceTask kMeansSourceTask = new KMeansWorker.KMeansSourceTask();
    KMeansWorker.KMeansAllReduceTask kMeansAllReduceTask = new KMeansWorker.KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelism);
    ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelism);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new KMeansWorker.CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
    kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
    return kmeansComputeGraphBuilder.build();
  }

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "KMeans Clustering Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(DataObjectConstants.WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("BatchScheduler-test");
    jobBuilder.setWorkerClass(BatchTaskSchedulerExample.class.getName());
    jobBuilder.addComputeResource(2, 2048, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    long startTime = System.currentTimeMillis();

    ComputeGraph firstGraph = buildFirstGraph(null, 0, parallelismValue, 2, config);

    ComputeGraph secondGraph = buildSecondGraph(null, 0, parallelismValue, 2, config);

    ComputeGraph thirdGraph = buildThirdGraph(parallelismValue, config);

    //Get the execution plan for the dependent task graphs
    /*Map<String, ExecutionPlan> taskSchedulePlanMap
    = cEnv.build(firstGraph, secondGraph, thirdGraph);*/

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);

    //Using the map we can get the execution plan for the individual task graphs
    //ExecutionPlan firstGraphExecutionPlan = taskSchedulePlanMap.get(firstGraph.getGraphName());

    //Actual execution for the first taskgraph
    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);

    //Retrieve the output of the first task graph
    DataObject<Object> firstGraphObject = taskExecutor.getOutput(
        firstGraph, firstGraphExecutionPlan, "datapointsink");

    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);

    //ExecutionPlan secondGraphExecutionPlan = taskSchedulePlanMap.get(secondGraph.getGraphName());

    taskExecutor.execute(secondGraph, secondGraphExecutionPlan);

    //Retrieve the output of the first task graph
    DataObject<Object> secondGraphObject = taskExecutor.getOutput(
        secondGraph, secondGraphExecutionPlan, "centroidsink");

    long endTimeData = System.currentTimeMillis();

    //Perform the iterations from 0 to 'n' number of iterations
    //ExecutionPlan plan = taskSchedulePlanMap.get(kmeansTaskGraph.getGraphName());
    ExecutionPlan plan = taskExecutor.plan(thirdGraph);
    taskExecutor.addInput(
        thirdGraph, plan, "kmeanssource", "points", firstGraphObject);
    taskExecutor.addInput(
        thirdGraph, plan, "kmeanssource", "centroids", secondGraphObject);
    //actual execution of the third task graph
    taskExecutor.execute(thirdGraph, plan);
    //TODO:Retrieve the output of the graph
    cEnv.close();

    long endTime = System.currentTimeMillis();

    LOG.info("Total Execution Time: " + (endTime - startTime)
        + "\tData Load time : " + (endTimeData - startTime)
        + "\tCompute Time : " + (endTime - endTimeData));
  }
}
