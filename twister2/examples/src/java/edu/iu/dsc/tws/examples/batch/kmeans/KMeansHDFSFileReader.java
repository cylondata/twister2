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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

/**
 * This class is responsible for reading the input from the HDFS. It internally uses the HDFS Utils
 * class to set the configuration values required for accessing the HDFS. Then, it will create the
 * hadoop file system object to access the HDFS.
 */
public class KMeansHDFSFileReader {

  private Config config;
  private HdfsUtils hdfsUtils;
  private HadoopFileSystem hadoopFileSystem;

  public KMeansHDFSFileReader(Config cfg) {
    this.config = cfg;
    hdfsUtils = new HdfsUtils(this.config);
    hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
  }

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing.
   * @param fName
   * @param dimension
   * @return
   */
  public double[][] readDataPoints(String fName, int dimension) {

    String fileName = HdfsDataContext.getHdfsDataDirectory(config) + "/" + fName;
    String directoryString = HdfsDataContext.getHdfsUrlDefault(config) + fileName;
    Path path = new Path(directoryString);

    BufferedReader bufferedReader = null;
    File f = new File(fName);
    try {
      if (hadoopFileSystem.exists(path)) {
        bufferedReader = new BufferedReader(new InputStreamReader(
            hadoopFileSystem.open(path)));
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    String line = "";
    int value = 0;
    int lengthOfFile = hdfsUtils.getLengthOfFile(fName);

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
        hadoopFileSystem.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    return dataPoints;
  }

  /**
   *  It reads the centroids from the corresponding file and store the data in a two-dimensional
   *  array for the later processing. The size of the two-dimensional array should be equal to the
   *  number of clusters and the dimension considered for the clustering process.
   * @param fileName
   * @param dimension
   * @param numberOfClusters
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

