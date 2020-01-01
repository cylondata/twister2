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
package edu.iu.dsc.tws.examples.csv;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

/**
 * It is the main class for the K-Means clustering which consists of four main tasks namely
 * generation of datapoints and centroids, partition and read the partitioned data points,
 * read the centroids, and finally perform the distance calculation.
 */
public class KMeansComputeJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(KMeansComputeJob.class.getName());

  /**
   * First, the execute method invokes the generateDataPoints method to generate the datapoints file
   * and centroid file based on the respective filesystem submitted by the user. Next, it invoke
   * the DataObjectSource and DataObjectSink to partition and read the partitioned data points
   * respectively through data points task graph. Then, it calls the DataFileReader to read the
   * centroid values from the filesystem through centroid task graph. Next, the datapoints are
   * stored in DataSet \(0th object\) and centroids are stored in DataSet 1st object\). Finally, it
   * constructs the kmeans task graph to perform the clustering process which computes the distance
   * between the centroids and data points.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void execute(Config config, int workerId, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    LOG.log(Level.FINE, "Task worker starting: " + workerId);

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerId, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    int parallelismValue = config.getIntegerValue(DataObjectConstants.PARALLELISM_VALUE);
    int dimension = config.getIntegerValue(DataObjectConstants.DIMENSIONS);
    int numFiles = config.getIntegerValue(DataObjectConstants.NUMBER_OF_FILES);
    int dsize = config.getIntegerValue(DataObjectConstants.DSIZE);
    int csize = config.getIntegerValue(DataObjectConstants.CSIZE);
    int iterations = config.getIntegerValue(DataObjectConstants.ARGS_ITERATIONS);

    String dataDirectory = config.getStringValue(DataObjectConstants.DINPUT_DIRECTORY) + workerId;
    String centroidDirectory = config.getStringValue(
        DataObjectConstants.CINPUT_DIRECTORY) + workerId;

    //String dataDirectory = config.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
    //String centroidDirectory = config.getStringValue(DataObjectConstants.CINPUT_DIRECTORY);

    KMeansUtils.generateDataPoints(config, dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    long startTime = System.currentTimeMillis();

    /* First Graph to partition and read the partitioned data points **/
    ComputeGraph datapointsTaskGraph = buildDataPointsTG(dataDirectory, dsize,
        parallelismValue, dimension, config);

    /* Second Graph to read the centroids **/
    ComputeGraph centroidsTaskGraph = buildCentroidsTG(centroidDirectory, csize,
        parallelismValue, dimension, config);

    /* Third Graph to do the actual calculation **/
    ComputeGraph kmeansTaskGraph = buildKMeansTG(parallelismValue, config);

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);

    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);

    //Get the execution plan for the second task graph
    //ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);

    //Actual execution for the second taskgraph
    //taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);

    long endTimeData = System.currentTimeMillis();

    //Perform the iterations from 0 to 'n' number of iterations
    /*IExecutor ex = taskExecutor.createExecution(kmeansTaskGraph);
    for (int i = 0; i < iterations; i++) {
      //actual execution of the third task graph
      ex.execute(i == iterations - 1);
    }*/
    cEnv.close();

    long endTime = System.currentTimeMillis();
    LOG.info("Total K-Means Execution Time: " + (endTime - startTime)
        + "\tData Load time : " + (endTimeData - startTime)
        + "\tCompute Time : " + (endTime - endTimeData));
  }

  public static ComputeGraph buildDataPointsTG(String dataDirectory, int dsize,
                                               int parallelismValue, int dimension,
                                               Config conf) {
    PointDataSource ps = new PointDataSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory, "points", dimension);
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    // Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", ps, parallelismValue);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);
    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
    return datapointsComputeGraphBuilder.build();
  }

  public static ComputeGraph buildCentroidsTG(String centroidDirectory, int csize,
                                              int parallelismValue, int dimension,
                                              Config conf) {
    PointDataSource cs = new PointDataSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory,
        "centroids", dimension);
    ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    centroidsComputeGraphBuilder.addSource("centroidsource", cs,
        parallelismValue);
    centroidsComputeGraphBuilder.setMode(OperationMode.BATCH);
    centroidsComputeGraphBuilder.setTaskGraphName("centTG");
    return centroidsComputeGraphBuilder.build();
  }


  public static ComputeGraph buildKMeansTG(int parallelismValue, Config conf) {
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addCompute(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
    kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
    return kmeansComputeGraphBuilder.build();
  }

  public static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataPartition<?> dataPartition = null;
    private DataPartition<?> centroidPartition = null;

    public KMeansSourceTask() {
    }

    @Override
    public void execute() {
      int dim = config.getIntegerValue("dim", 2);
      double[][] datapoints = (double[][]) dataPartition.first();
      double[][] centroid = (double[][]) centroidPartition.first();
      double[][] kMeansCenters = KMeansUtils.findNearestCenter(dim, datapoints, centroid);
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @Override
    public void add(String name, DataPartition<?> data) {
      if ("points".equals(name)) {
        this.dataPartition = data;
      }
      if ("centroids".equals(name)) {
        this.centroidPartition = data;
      }
    }

    @Override
    public IONames getReceivableNames() {
      return IONames.declare("points", "centroids");
    }
  }

  public static class KMeansAllReduceTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;
    private double[][] newCentroids;

    public KMeansAllReduceTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      double[][] centroids = (double[][]) message.getContent();
      newCentroids = new double[centroids.length][centroids[0].length - 1];
      for (int i = 0; i < centroids.length; i++) {
        for (int j = 0; j < centroids[0].length - 1; j++) {
          double newVal = centroids[i][j] / centroids[i][centroids[0].length - 1];
          newCentroids[i][j] = newVal;
        }
      }
      return true;
    }

    @Override
    public DataPartition<double[][]> get() {
      return new EntityPartition<>(newCentroids);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare("centroids");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

  /**
   * This class aggregates the cluster centroid values and sum the new centroid values.
   */
  public static class CentroidAggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    public CentroidAggregator() {
    }

    /**
     * The actual message callback
     *
     * @param object1 the actual message
     * @param object2 the actual message
     */
    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[][] kMeansCenters = (double[][]) object1;
      double[][] kMeansCenters1 = (double[][]) object2;

      double[][] newCentroids = new double[kMeansCenters.length][kMeansCenters[0].length];

      if (kMeansCenters.length != kMeansCenters1.length) {
        throw new RuntimeException("Center sizes not equal " + kMeansCenters.length
            + " != " + kMeansCenters1.length);
      }

      for (int j = 0; j < kMeansCenters.length; j++) {
        for (int k = 0; k < kMeansCenters[0].length; k++) {
          double newVal = kMeansCenters[j][k] + kMeansCenters1[j][k];
          newCentroids[j][k] = newVal;
        }
      }
      return newCentroids;
    }
  }
}

