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
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansFileReader;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

public abstract class CentroidParallelSourceImpl extends BaseSource implements Receptor {

  private static final Logger LOG
      = Logger.getLogger(CentroidParallelSourceImpl.class.getName());

  private static final long serialVersionUID = -1L;

  private String centroidDirectory;
  private String fileSystem;
  private int numberOfClusters;
  private int dimension;

  private KMeansFileReader kMeansFileReader;

  private DataObject<double[][]> centroids = null;

  public CentroidParallelSourceImpl() {
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
    centroids.addPartition(new EntityPartition<>(0, centroid));
    LOG.info("Centroid Values are:::" + Arrays.deepToString(centroid));
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    kMeansFileReader = new KMeansFileReader(config, fileSystem);
    centroidDirectory = cfg.getStringValue(KMeansConstants.ARGS_CINPUT_DIRECTORY);
    fileSystem = cfg.getStringValue(KMeansConstants.ARGS_FILE_SYSTEM);
    dimension = Integer.parseInt(cfg.getStringValue(KMeansConstants.ARGS_DIMENSIONS));
    numberOfClusters = Integer.parseInt(cfg.getStringValue(KMeansConstants.
        ARGS_NUMBER_OF_CLUSTERS));
    centroids = new DataObjectImpl<>(config);
  }

  @Override
  public void add(String name, DataObject<?> data) {
  }
}
