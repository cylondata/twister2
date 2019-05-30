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

package edu.iu.dsc.tws.examples.batch.kmeans.ftolerance;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskEnvironment;
import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.api.task.ftolerance.CheckpointingWorkerEnv;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.dfw.io.types.ObjectPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerPacker;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.ftolerance.api.Snapshot;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

/**
 * It is the main class for the K-Means clustering which consists of four main tasks namely
 * generation of datapoints and centroids, partition and read the partitioned data points,
 * read the centroids, and finally perform the distance calculation.
 */
public class KMeansCheckpointingWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(KMeansCheckpointingTaskWorker.class.getName());

  private static final String I_KEY = "iter";
  private static final String CENT_OBJ = "centDataObj";

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
  public void execute(Config config,
                      int workerId,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    TaskEnvironment taskEnv = TaskEnvironment.init(config, workerId, workerController,
        volatileVolume);

    CheckpointingWorkerEnv checkpointingEnv =
        CheckpointingWorkerEnv.newBuilder(config, workerId, workerController)
            .registerVariable(I_KEY, IntegerPacker.getInstance())
            .registerVariable(CENT_OBJ, ObjectPacker.getInstance())
            .build();

    Snapshot snapshot = checkpointingEnv.getSnapshot();

    TaskExecutor taskExecutor = taskEnv.getTaskExecutor();

    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory() + workerId;
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory() + workerId;

    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    long startTime = System.currentTimeMillis();

    /* First Graph to partition and read the partitioned data points **/
    DataFlowTaskGraph datapointsTaskGraph = KMeansWorker.buildDataPointsTG(dataDirectory, dsize,
        parallelismValue, dimension, config);
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");

    DataObject<Object> centroidsDataObject;
    if (!snapshot.isCheckpointed(CENT_OBJ)) {
      /* Second Graph to read the centroids **/
      DataFlowTaskGraph centroidsTaskGraph = KMeansWorker.buildCentroidsTG(centroidDirectory, csize,
          parallelismValue, dimension, config);
      //Get the execution plan for the second task graph
      ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
      //Actual execution for the second taskgraph
      taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);
      //Retrieve the output of the first task graph
      centroidsDataObject = taskExecutor.getOutput(
          centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");
    } else {
      centroidsDataObject = (DataObject<Object>) snapshot.get(CENT_OBJ);
    }

    long endTimeData = System.currentTimeMillis();


    /* Third Graph to do the actual calculation **/
    DataFlowTaskGraph kmeansTaskGraph = KMeansWorker.buildKMeansTG(parallelismValue, config);

    //Perform the iterations from 0 to 'n' number of iterations
    ExecutionPlan plan = taskExecutor.plan(kmeansTaskGraph);

    int i = (int) snapshot.getOrDefault(I_KEY, 0);      // recover from checkpoint
    for (; i < iterations; i++) {
      //add the datapoints and centroids as input to the kmeanssource task.
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
      //actual execution of the third task graph
      taskExecutor.itrExecute(kmeansTaskGraph, plan);
      //retrieve the new centroid value for the next iterations
      centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");

      // at each iteration, commit the checkpoint.
      checkpointingEnv.commitSnapshot();
    }
    taskExecutor.waitFor(kmeansTaskGraph, plan);

    DataPartition<?> centroidPartition = centroidsDataObject.getPartitions(workerId);
    double[][] centroid = (double[][]) centroidPartition.getConsumer().next();
    long endTime = System.currentTimeMillis();
    if (workerId == 0) {
      LOG.info("Data Load time : " + (endTimeData - startTime) + "\n"
          + "Total Time : " + (endTime - startTime)
          + "Compute Time : " + (endTime - endTimeData));
    }
    LOG.info("Final Centroids After\t" + iterations + "\titerations\t"
        + Arrays.deepToString(centroid));
  }


}

