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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;

public class KMeansWorkerUtils {

  private static final Logger LOG = Logger.getLogger(KMeansWorkerUtils.class.getName());

  private Config config;

  public KMeansWorkerUtils(Config cfg) {
    this.config = cfg;
  }

  public double[][] getDataPoints(int workerId, DataPartition<Object> dataPointsPartition,
                                  int dsize, int parallel, int dimension) {
    DataObjectImpl<Object> dataObject
        = (DataObjectImpl<Object>) dataPointsPartition.getConsumer().next();
    Iterator<ArrayList> arrayListIterator
        = (Iterator<ArrayList>) dataObject.getPartitions(workerId).getConsumer().next();
    double[][] datapoint = new double[dsize / parallel + 1][dimension];
    int value = 0;
    while (arrayListIterator.hasNext()) {
      String val = String.valueOf(arrayListIterator.next());
      String[] data = val.split(",");
      for (int i = 0; i < dimension; i++) {
        datapoint[value][i] = Double.parseDouble(data[i].trim());
      }
      value++;
    }
    return datapoint;
  }

  public double[][] getCentroids(int workerId, DataPartition<Object> centroidsPartition,
                                 int csize, int parallel, int dimension) {
    DataObjectImpl<Object> dataObject
        = (DataObjectImpl<Object>) centroidsPartition.getConsumer().next();
    Iterator<ArrayList> arrayListIterator
        = (Iterator<ArrayList>) dataObject.getPartitions(workerId).getConsumer().next();
    double[][] datapoint = new double[csize / parallel + 1][dimension];
    int value = 0;
    while (arrayListIterator.hasNext()) {
      String val = String.valueOf(arrayListIterator.next());
      String[] data = val.split(",");
      for (int i = 0; i < dimension; i++) {
        datapoint[value][i] = Double.parseDouble(data[i].trim());
      }
      value++;
    }
    return datapoint;
  }

  /**
   * This method is to generate the datapoints and centroids based on the user submitted values.
   */
  public boolean generateDatapoints(int dimension, int numFiles, int dsize, int csize,
                                    String dinputDirectory, String cinputDirectory) {
    try {
      KMeansDataGenerator.generateData("txt", new Path(dinputDirectory),
          numFiles, dsize, 100, dimension, config);
      KMeansDataGenerator.generateData("txt", new Path(cinputDirectory),
          numFiles, csize, 100, dimension, config);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create input data:", ioe);
    }
    return true;
  }
}
