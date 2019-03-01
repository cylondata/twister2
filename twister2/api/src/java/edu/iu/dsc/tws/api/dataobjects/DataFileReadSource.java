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
package edu.iu.dsc.tws.api.dataobjects;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataFileReadSource extends BaseSource implements Collector {

  private static final Logger LOG
      = Logger.getLogger(DataFileReadSource.class.getName());

  private static final long serialVersionUID = -1L;

  private String fileDirectory;
  private int numberOfCenters;
  private int dimension;

  private DataFileReader fileReader;

  private DataObject<double[][]> centroids = null;

  private double[][] centroid = null;

  public DataFileReadSource() {
  }

  @Override
  public void execute() {
    LOG.info("Context Task Index:" + context.taskIndex() + "file directory:" + fileDirectory);
    centroid = fileReader.readCentroids(new Path(fileDirectory), dimension);
    centroids.addPartition(new EntityPartition<>(0, centroid));
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    String fileSystem = cfg.getStringValue(DataObjectConstants.ARGS_FILE_SYSTEM);
    fileReader = new DataFileReader(config, fileSystem);
    fileDirectory = cfg.getStringValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_DIMENSIONS));
    numberOfCenters = Integer.parseInt(cfg.getStringValue(DataObjectConstants.
        ARGS_NUMBER_OF_CLUSTERS));
    centroids = new DataObjectImpl<>(config);
  }

  @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), centroid);
  }
}
