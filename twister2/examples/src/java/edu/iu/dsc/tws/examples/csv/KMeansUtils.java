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

import java.io.IOException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;

/**
 * This class has the utility methods to generate the datapoints and centroids. Also, it has the
 * methods to parse the data partitions and data objects and store into the double arrays.
 */
public final class KMeansUtils {
  private KMeansUtils() {
  }

  /**
   * This method is to generate the datapoints and centroids based on the user submitted values.
   */
  public static void generateDataPoints(Config config, int dim, int numFiles, int datasize,
                                        int centroidsize, String dinputDirectory,
                                        String cinputDirectory, String type) {
    try {
      System.out.println("type of file to be generated:" + type);
      KMeansDataGenerator.generateData(type, new Path(dinputDirectory),
          numFiles, datasize, 100, dim, config);
      KMeansDataGenerator.generateData(type, new Path(cinputDirectory),
          numFiles, centroidsize, 100, dim, config);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create input data:", ioe);
    }
  }

  /**
   * This method first invoke the euclidean distance between to calculate the distance between the
   * data points assigned to the task (i.e from start index to end index) and the centroid values.
   * The calculated centroid values and the number of data points closer to the particular centroid
   * values assigned to the centerSums array object.
   */
  public static double[][] findNearestCenter(int dim, double[][] datapoints, double[][] centers) {
    double[][] centerSums = new double[centers.length][centers[0].length + 1];
    for (int i = 0; i < datapoints.length; i++) {
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
    return centerSums;
  }

  /**
   * This method calculates the distance between the datapoint and the centroid value. The value 1
   * represents the data point value, value 2 represents the centroid value, and the length
   * represents the dimension of the data point and centroid values.
   */
  private static double calculateEuclideanDistance(double[] value1, double[] value2, int length) {
    double sum = 0.0;
    for (int i = 0; i < length; i++) {
      double v = (value1[i] - value2[i]) * (value1[i] - value2[i]);
      sum += v;
    }
    return sum;
  }
}
