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
package edu.iu.dsc.tws.examples.batch.arrow;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.data.utils.ArrowDataFileWriter;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class ArrowComputeJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(ArrowComputeJob.class.getName());

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

    ArrowDataFileWriter arrowDataFileWriter
        = new ArrowDataFileWriter(config, "/tmp/test.arrow");
    arrowDataFileWriter.setUpwriteArrowFile(new Path("/tmp/test.arrow"), true);
    long startTime = System.currentTimeMillis();

//    /* First Graph to partition and read the partitioned data points **/
//    ComputeGraph datapointsTaskGraph = buildDataPointsTG(dataDirectory, dsize,
//        parallelismValue, dimension, config, type);
//
//    //Get the execution plan for the first task graph
//    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
//
//    //Actual execution for the first taskgraph
//    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);

    long endTimeData = System.currentTimeMillis();
    cEnv.close();
    long endTime = System.currentTimeMillis();
    LOG.info("Total K-Means Execution Time: " + (endTime - startTime)
        + "\tData Load time : " + (endTimeData - startTime)
        + "\tCompute Time : " + (endTime - endTimeData));
  }

//  public static ComputeGraph buildDataPointsTG(String dataDirectory, int dsize,
//                                               int parallelismValue, int dimension,
//                                               Config conf, String filetype) {
//    PointDataSource ps = new PointDataSource(Context.TWISTER2_DIRECT_EDGE,
//        dataDirectory, "points", dimension, dsize, filetype);
//    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
//
//    // Add source, compute, and sink tasks to the task graph builder for the first task graph
//    datapointsComputeGraphBuilder.addSource("datapointsource", ps, parallelismValue);
//    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);
//    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
//    return datapointsComputeGraphBuilder.build();
//  }
}
