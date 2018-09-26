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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.logging.Logger;

/**
 * This class is responsible for reading the input datapoints and the centroid values from the local
 * file system.
 */
public class KMeansFileReader {

  private static final Logger LOG = Logger.getLogger(KMeansFileReader.class.getName());

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing.
   * @param fName
   * @param dimension
   * @return
   */
  public double[][] readDataPoints(String fName, int dimension) {

    BufferedReader bufferedReader = null;
    File f = new File(fName);
    try {
      bufferedReader = new BufferedReader(new FileReader(f));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    String line = "";
    int value = 0;
    int lengthOfFile = getNumberOfLines(fName);
    double[][] dataPoints = new double[lengthOfFile][dimension];
    try {
      while ((line = bufferedReader.readLine()) != null) {
        String[] data = line.split(",");
        for (int i = 0; i < dimension; i++) {
          dataPoints[value][i] = Double.parseDouble(data[i].trim());
        }
        value++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bufferedReader.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    return dataPoints;
  }

  /**
   * It calculates the number of lines in the file name.
   * @param fileName
   * @return
   */
  public int getNumberOfLines(String fileName) {

    int numberOfLines = 0;
    try {
      File file = new File(fileName);
      LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
      lineNumberReader.skip(Long.MAX_VALUE);
      numberOfLines = lineNumberReader.getLineNumber();
      lineNumberReader.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return numberOfLines;
  }

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing.
   * @param fileName
   * @param dimension
   * @return
   */
  public double[][] readCentroids(String fileName, int dimension, int numberOfClusters) {

    double[][] centroids = new double[numberOfClusters][dimension];
    BufferedReader bufferedReader = null;
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

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        bufferedReader.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    return centroids;
  }
}

