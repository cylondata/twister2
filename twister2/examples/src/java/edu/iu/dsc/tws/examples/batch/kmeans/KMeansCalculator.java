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

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * This class is responsible for calculating the distance values between the datapoints and the
 * centroid values. The calculated new centroid values are stored in the KMeansCenters centersums
 * object.
 */
public class KMeansCalculator {

  private static final Logger LOG = Logger.getLogger(KMeansCalculator.class.getName());

  private double[][] points;
  private double[][] centerSums;
  private double[][] centroids;

  private int[] centerCounts;

  private int taskId;
  private int dimension;

  private int startIndex;
  private int endIndex;

  public KMeansCalculator(double[][] points, double[][] centres, int taskId, int dim,
                          int sIndex, int eIndex) {
    this.points = points;
    this.centroids = centres;
    this.taskId = taskId;
    this.dimension = dim;
    this.centerSums = new double[this.centroids.length][this.centroids[0].length + 1];
    this.centerCounts = new int[this.centroids.length];
    this.startIndex = sIndex;
    this.endIndex = eIndex;
  }

  /**
   * This method invokes the findnearestcenter method to find the datapoints closer to the centroid
   * values. The calculated value is assigned to the KMeansCenters object and return the same.
   * @return
   */
  public KMeansCenters calculate() {
    findNearestCenter(dimension, points, centroids);
    KMeansCenters kMeansCenters = new KMeansCenters(centerSums);
    return kMeansCenters;
  }

  /**
   * This method first invoke the euclidean distance between to calculate the distance between the
   * data points assigned to the task (i.e from start index to end index) and the centroid values.
   * The calculated centroid values and the number of data points closer to the particular centroid
   * values assigned to the centerSums array object.
   * @param dim
   * @param datapoints
   * @param centers
   * @return
   */
  public double[][] findNearestCenter(int dim, double[][] datapoints,
                                      double[][] centers) {
    LOG.fine("Start index:" + startIndex + "\tend index:" + endIndex);
    for (int i = startIndex; i < endIndex; i++) {
      int minimumCentroid = 0;
      double minValue = 0;
      double distance;

      //Calculate the distance between the datapoints and the centroids
      for (int j = 0; j < centers.length; j++) {
        distance = calculateEuclideanDistance(datapoints[i], centers[j], dim);
        if (j == 0) {
          minValue = distance;
        }
        if (distance < minValue) {
          minValue = distance;
          minimumCentroid = j;
        }
      }
      //Accumulate the values
      for (int k = 0; k < dim; k++) {
        centerSums[minimumCentroid][k] += datapoints[i][k];
      }
      centerSums[minimumCentroid][dim] += 1;
    }
    LOG.fine("Kmeans centroid values:" + Arrays.deepToString(centerSums));
    return centerSums;
  }

  /**
   * This method calculates the distance between the datapoint and the centroid value.
   * @param value1
   * @param value2
   * @param length
   * @return
   */
  public double calculateEuclideanDistance(double[] value1, double[] value2, int length) {
    double sum = 0;
    for (int i = 0; i < length; i++) {
      sum += (value1[i] - value2[i]) * (value1[i] - value2[i]);
    }
    return sum;
  }
}
