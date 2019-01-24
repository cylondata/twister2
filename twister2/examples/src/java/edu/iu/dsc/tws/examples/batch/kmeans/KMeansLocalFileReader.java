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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class is responsible for reading the input datapoints and the centroid values from the local
 * file system.
 */
public class KMeansLocalFileReader {

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing.
   */
  public double[][] readDataPoints(String fName, int dimension, String filesystem) {

    BufferedReader bufferedReader = KMeansUtils.getBufferedReader(null, fName, filesystem);
    int lengthOfFile = KMeansUtils.getNumberOfLines(fName);
    double[][] dataPoints = new double[lengthOfFile][dimension];

    try {
      int value = 0;
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] data = line.split(",");
        for (int i = 0; i < dimension; i++) {
          dataPoints[value][i] = Double.parseDouble(data[i].trim());
        }
        value++;
      }
      bufferedReader.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Error while reading datapoints", ioe);
    } finally {
      KMeansUtils.readClose();
    }
    return dataPoints;
  }

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing. The size of the two-dimensional array should be equal to the
   * number of clusters and the dimension considered for the clustering process.
   */
  public double[][] readCentroids(String fileName, int dimension, int numberOfClusters) {

    double[][] centroids = new double[numberOfClusters][dimension];
    BufferedReader bufferedReader;
    try {
      int value = 0;
      bufferedReader = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] data = line.split(",");
        for (int i = 0; i < dimension - 1; i++) {
          centroids[value][i] = Double.parseDouble(data[i].trim());
          centroids[value][i + 1] = Double.parseDouble(data[i + 1].trim());
        }
        value++;
      }
      bufferedReader.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Error while reading centroids", ioe);
    }
    return centroids;
  }
}

