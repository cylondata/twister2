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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This class acts as an interface for reading the input datapoints and centroid values from
 * the local file system or from the distributed file system (HDFS).
 */
public class DataFileReader {

  private static final Logger LOG = Logger.getLogger(DataFileReader.class.getName());

  private final Config config;
  private final String fileSystem;

  public DataFileReader(Config cfg, String fileSys) {
    this.config = cfg;
    this.fileSystem = fileSys;
  }

  /**
   * It reads the datapoints from the corresponding file system and store the data in a two
   * -dimensional array for the later processing.
   */
  public double[][] readCentroids(String fileName, int dimension, int numberOfClusters) {
    double[][] centroids = readCentroids(fileName, dimension, numberOfClusters, fileSystem);
    return centroids;
  }

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing. The size of the two-dimensional array should be equal to the
   * number of clusters and the dimension considered for the clustering process.
   */
  public double[][] readCentroids(String fileName, int dimension, int numberOfClusters,
                                  String filesystem) {

    double[][] centroids = new double[numberOfClusters][dimension];
    BufferedReader bufferedReader = DataObjectUtils.getBufferedReader(this.config,
        fileName, filesystem);
    try {
      int value = 0;
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] data = line.split(",");
        for (int i = 0; i < dimension - 1; i++) {
          centroids[value][i] = Double.parseDouble(data[i].trim());
          centroids[value][i + 1] = Double.parseDouble(data[i + 1].trim());
        }
        value++;
      }
      LOG.info("Centroid values are::::" + Arrays.deepToString(centroids));
    } catch (IOException ioe) {
      DataObjectUtils.readClose();
      throw new RuntimeException("Error while reading centroids", ioe);
    } finally {
      DataObjectUtils.readClose();
    }
    return centroids;
  }
}

