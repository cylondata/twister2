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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

/**
 * This class is to generate the datapoints and centroid values in a random manner and write the
 * datapoints and centroid values in the file.
 */
public class KMeansDataGenerator {

  protected KMeansDataGenerator() {
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of data
   * points, required dimension, minimum and maximum value (for the random number generation).
   * @param fileName
   * @param numPoints
   * @param dimension
   * @param minValue
   * @param maxValue
   */
  public static void generateDataPointsFile(String fileName, int numPoints, int dimension,
                                            int minValue, int maxValue) {
    try {
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));
      for (int i = 0; i < numPoints; i++) {
        String line = "";
        for (int j = 0; j < dimension; j++) {
          Random r = new Random();
          line += minValue + (maxValue - minValue) * r.nextDouble();
          if (j == 0) {
            line += "," + "\t";
          }
        }
        line += "\n";
        bufferedWriter.write(line);
      }
      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of
   * centroids, required dimension, minimum and maximum value (for the random number generation).
   * @param fileName
   * @param numCentroids
   * @param dimension
   * @param minValue
   * @param maxValue
   */

  public static void generateCentroidFile(String fileName, int numCentroids, int dimension,
                                          int minValue, int maxValue) {
    try {
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));
      for (int i = 0; i < numCentroids; i++) {
        String line = "";
        for (int j = 0; j < dimension; j++) {
          Random r = new Random();
          line += minValue + (maxValue - minValue) * r.nextDouble();
          if (j == 0) {
            line += "," + "\t";
          }
        }
        line += "\n";
        bufferedWriter.write(line);
      }
      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
