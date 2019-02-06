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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.cdfw.task.ConnectedSource;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansFileReader;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansCentroidParallelWorker extends TaskWorker {

  @Override
  public void execute() {
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int csize = kMeansJobParameters.getCsize();

    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();

    try {
      if (workerId == 0) {
        KMeansDataGenerator.generateData(
            "txt", new Path(cinputDirectory), numFiles, csize, 100, dimension);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Centroid Files Not able to Generate" + ioe);
    }

    KMeansCentroidParallelTask task = new KMeansCentroidParallelTask();
    taskGraphBuilder.addSource("map", task, parallelismValue);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, plan);
  }

  public static class KMeansCentroidParallelTask extends ConnectedSource {

    private static final Logger LOG
        = Logger.getLogger(KMeansCentroidParallelWorker.class.getName());

    private static final long serialVersionUID = -1L;

    private String centroidDirectory;
    private String fileSystem;
    private int numberOfClusters;
    private int dimension;

    private KMeansFileReader kMeansFileReader;

    public KMeansCentroidParallelTask() {
    }

    @Override
    public void execute() {
      LOG.info("Context Task Index:" + context.taskIndex());
      File[] files = new File(centroidDirectory).listFiles();
      String fileName = null;
      for (File file : files) {
        fileName = file.getName();
      }
      double[][] centroid = kMeansFileReader.readCentroids(
          centroidDirectory + "/" + fileName, dimension, numberOfClusters);

      DataObject<double[][]> centroids = new DataObjectImpl<>(config);
      centroids.addPartition(new EntityPartition<>(0, centroid));

      LOG.info("Centroid Values are:::" + Arrays.deepToString(centroid));
      //context.writeEnd("partition", centroids);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      kMeansFileReader = new KMeansFileReader(config, fileSystem);
      centroidDirectory = cfg.getStringValue(KMeansConstants.ARGS_CINPUT_DIRECTORY);
      fileSystem = cfg.getStringValue(KMeansConstants.ARGS_FILE_SYSTEM);
      dimension = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));
      numberOfClusters = Integer.parseInt(cfg.getStringValue(KMeansConstants.
          ARGS_NUMBER_OF_CLUSTERS));
    }
  }

}
