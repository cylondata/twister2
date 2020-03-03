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
    int dimension = config.getIntegerValue(DataObjectConstants.WORKERS);

    ArrowDataFileWriter arrowDataFileWriter = new ArrowDataFileWriter(config, "/tmp/test.arrow");
    arrowDataFileWriter.setUpwriteArrowFile(new Path("/tmp/test.arrow"), true);

    long startTime = System.currentTimeMillis();
    long endTimeData = System.currentTimeMillis();
    cEnv.close();
    long endTime = System.currentTimeMillis();
    LOG.info("Total K-Means Execution Time: " + (endTime - startTime)
        + "\tData Load time : " + (endTimeData - startTime)
        + "\tCompute Time : " + (endTime - endTimeData));
  }
}
