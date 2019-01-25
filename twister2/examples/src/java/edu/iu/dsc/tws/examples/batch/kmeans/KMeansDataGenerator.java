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

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This class is to generate the datapoints and centroid values in a random manner and write the
 * datapoints and centroid values in the local filesystem or the distributed filesystem.
 */
public class KMeansDataGenerator {

  protected KMeansDataGenerator() {
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of data
   * points, required dimension, minimum and maximum value for the random points generation.
   */
  public static void generateDataPointsFile(String fileName, int numPoints, int dimension,
                                            int seedValue, Config config, String fileSys) {
    String datapoints = buildPoints(numPoints, dimension, seedValue);
    writeToPointsFile(datapoints, fileName, config, fileSys);
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of
   * centroids, required dimension, minimum and maximum value for the random points generation.
   */
  public static void generateCentroidFile(String fileName, int numCentroids, int dimension,
                                          int seedValue, Config config, String fileSys) {
    String centroids = buildPoints(numCentroids, dimension, seedValue);
    writeToPointsFile(centroids, fileName, config, fileSys);
  }

  /**
   * This method is to build the datapoints/centroid values which is based on the number of
   * data points, dimension, and their respective seed value.
   */
  private static String buildPoints(int numPoints, int dimension, int seedValue) {

    StringBuilder datapoints = new StringBuilder();
    Random r = new Random(seedValue);
    for (int i = 0; i < numPoints; i++) {
      String line = "";
      for (int j = 0; j < dimension; j++) {
        double randomValue = r.nextDouble();
        line = line + randomValue;
        if (j == 0) {
          line = line + "," + "\t";
        }
      }
      datapoints.append(line).append("\n");
    }
    return datapoints.toString();

  }

  /**
   * This method writes the data points into the local filesystem or the distributed file system
   * which is based on the user preferred value while submitting the job.
   */
  private static void writeToPointsFile(String datapoints, String fileName, Config config,
                                        String fileSystem) {
    StringTokenizer stringTokenizer = new StringTokenizer(datapoints, "\n");
    try {
      BufferedWriter bufferedWriter = KMeansUtils.getBufferedWriter(config, fileName, fileSystem);
      while (stringTokenizer.hasMoreTokens()) {
        String out = stringTokenizer.nextToken().trim();
        bufferedWriter.write(out);
        bufferedWriter.write("\n");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("File Writing Exception", ioe);
    } finally {
      KMeansUtils.writeClose();
    }
  }
}
