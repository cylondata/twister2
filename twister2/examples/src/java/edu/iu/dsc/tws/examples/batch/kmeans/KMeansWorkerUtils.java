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
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;

/**
 * This class has the utility methods to parse the data partitions and data objects.
 */
public class KMeansWorkerUtils {

  private static final Logger LOG = Logger.getLogger(KMeansWorkerUtils.class.getName());

  private Config config;

  private int dimension;
  private int parallel;
  private int dsize;
  private int csize;

  private double[][] datapoint = null;
  private double[][] centroid = null;

  public KMeansWorkerUtils(Config cfg) {
    this.config = cfg;
    this.parallel = Integer.parseInt(config.getStringValue("parallelism"));
    this.dsize = Integer.parseInt(config.getStringValue("dsize"));
    this.csize = Integer.parseInt(config.getStringValue("csize"));
    this.dimension = Integer.parseInt(config.getStringValue("dim"));

    datapoint = new double[dsize / parallel + 1][dimension];
    centroid = new double[csize][dimension];
  }

  /**
   * This method receive datapoints object and parse the object and store it into the double array
   * based on the delimiter comma.
   */
  public double[][] getDataPoints(int taskIndex, DataObject<?> datapointsDataObject) {

    Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
        datapointsDataObject.getPartitions(taskIndex).getConsumer().next();

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
   * This method receive centroids object and parse the object and store it into the double array
   * based on the delimiter comma.
   */
  public double[][] getCentroids(int taskIndex, DataObject<?> centroidsDataObject) {

    Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
        centroidsDataObject.getPartitions(taskIndex).getConsumer().next();

    int value = 0;
    while (arrayListIterator.hasNext()) {
      String val = String.valueOf(arrayListIterator.next());
      String[] data = val.split(",");
      for (int i = 0; i < dimension; i++) {
        centroid[value][i] = Double.parseDouble(data[i].trim());
      }
      value++;
    }
    return centroid;
  }

  /**
   * This method receive datapartitions and parse partition and store it into the double array
   * based on the delimiter comma.
   */
  public double[][] getDataPoints(int partitionId, DataPartition<Object> dataPointsPartition) {
    DataObjectImpl<Object> dataObject
        = (DataObjectImpl<Object>) dataPointsPartition.getConsumer().next();
    Iterator<ArrayList> arrayListIterator
        = (Iterator<ArrayList>) dataObject.getPartitions(partitionId).getConsumer().next();
    double[][] points = new double[dsize / parallel + 1][dimension];
    int value = 0;
    while (arrayListIterator.hasNext()) {
      String val = String.valueOf(arrayListIterator.next());
      String[] data = val.split(",");
      for (int i = 0; i < dimension; i++) {
        points[value][i] = Double.parseDouble(data[i].trim());
      }
      value++;
    }
    return points;
  }


  /**
   * This method receive centroids object and parse the object and store it into the double array
   * based on the delimiter comma.
   */
  public double[][] getCentroids(int partitionId, DataPartition<Object> centroidsPartition) {
    DataObjectImpl<Object> dataObject
        = (DataObjectImpl<Object>) centroidsPartition.getConsumer().next();
    double[][] centroids = new double[csize][dimension];
    Iterator<ArrayList> arrayListIterator
        = (Iterator<ArrayList>) dataObject.getPartitions(partitionId).getConsumer().next();
    int value = 0;
    while (arrayListIterator.hasNext()) {
      String val = String.valueOf(arrayListIterator.next());
      String[] data = val.split(",");
      for (int i = 0; i < dimension; i++) {
        centroids[value][i] = Double.parseDouble(data[i].trim());
      }
      value++;
    }
    return centroids;
  }

  /**
   * This method is to generate the datapoints and centroids based on the user submitted values.
   */
  public boolean generateDatapoints(int dim, int numFiles, int datasize, int centroidsize,
                                    String dinputDirectory, String cinputDirectory) {
    try {
      KMeansDataGenerator.generateData("txt", new Path(dinputDirectory),
          numFiles, datasize, 100, dim, config);
      KMeansDataGenerator.generateData("txt", new Path(cinputDirectory),
          numFiles, centroidsize, 100, dim, config);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create input data:", ioe);
    }
    return true;
  }
}
