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
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

/**
 * This class is to generate the datapoints and centroid values in a random manner and write the
 * datapoints and centroid values in the file.
 */
public class KMeansDataGenerator {

  private static final Logger LOG = Logger.getLogger(KMeansDataGenerator.class.getName());

  protected KMeansDataGenerator() {
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of data
   * points, required dimension, minimum and maximum value (for the random number generation).
   */
  public static void generateDataPointsFile(String fileName, int numPoints, int dimension,
                                            int seedValue, Config config, String fileSys) {

    StringBuffer datapoints = new StringBuffer();
    Random r = new Random(seedValue);
    try {
      for (int i = 0; i < numPoints; i++) {
        String line = "";
        for (int j = 0; j < dimension; j++) {
          double randomValue = r.nextDouble();
          line += randomValue;
          if (j == 0) {
            line += "," + "\t";
          }
        }
        datapoints.append(line);
        datapoints.append("\n");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    writeToPointsFile(datapoints.toString(), fileName, config, fileSys);
  }

  /**
   * This method generates the datapoints which is based on the which is based on the number of
   * centroids, required dimension, minimum and maximum value (for the random number generation).
   */
  public static void generateCentroidFile(String fileName, int numCentroids, int dimension,
                                          int seedValue, Config config, String fileSys) {

    StringBuffer centroids = new StringBuffer();
    Random r = new Random(seedValue);
    try {
      for (int i = 0; i < numCentroids; i++) {
        String line = "";
        for (int j = 0; j < dimension; j++) {
          double randomValue = r.nextDouble();
          line += randomValue;
          if (j == 0) {
            line += "," + "\t";
          }
        }
        centroids.append(line);
        centroids.append("\n");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    writeToCentroidFile(centroids.toString(), fileName, config, fileSys);
  }

  /**
   * This method writes the data points into the local filesystem or HDFS which is based on the user
   * submitted value.
   */
  private static void writeToPointsFile(String datapoints, String fileName, Config config,
                                        String fileSystem) {

    BufferedWriter bufferedWriter = null;
    StringTokenizer stringTokenizer = new StringTokenizer(datapoints, "\n");

    HadoopFileSystem hadoopFileSystem = null;
    HadoopDataOutputStream dataOutputStream = null;
    try {
      if ("hdfs".equals(fileSystem)) {
        HdfsUtils hdfsUtils = new HdfsUtils(config, fileName);
        hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
        Path path = hdfsUtils.getPath();

        if (!hadoopFileSystem.exists(path)) {
          dataOutputStream = hadoopFileSystem.create(path);
          bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"));
          while (stringTokenizer.hasMoreTokens()) {
            String out = stringTokenizer.nextToken().trim();
            bufferedWriter.write(out);
            bufferedWriter.write("\n");
          }
        } else {
          throw new RuntimeException("File already exists in the hdfs, remove it from hdfs");
        }
      } else if ("local".equals(fileSystem)) {
        File file = new File(fileName);
        if (!file.exists()) {
          //file.createNewFile();
          bufferedWriter = new BufferedWriter(new FileWriter(fileName));
          while (stringTokenizer.hasMoreTokens()) {
            bufferedWriter.write(stringTokenizer.nextToken().trim());
            bufferedWriter.write("\n");
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        bufferedWriter.flush();
        bufferedWriter.close();
        if (dataOutputStream != null) {
          dataOutputStream.close();
        }
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * This method writes the centroids into the local filesystem or HDFS which is based on the user
   * submitted value.
   */
  private static void writeToCentroidFile(String datapoints, String fileName,
                                          Config config, String fileSystem) {
    BufferedWriter bufferedWriter = null;
    StringTokenizer stringTokenizer = new StringTokenizer(datapoints, "\n");
    HadoopFileSystem hadoopFileSystem = null;
    HadoopDataOutputStream dataOutputStream = null;
    try {
      if ("hdfs".equals(fileSystem)) {
        HdfsUtils hdfsUtils = new HdfsUtils(config, fileName);
        hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
        Path path = hdfsUtils.getPath();

        if (!hadoopFileSystem.exists(path)) {
          dataOutputStream = hadoopFileSystem.create(path);
          bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"));

          while (stringTokenizer.hasMoreTokens()) {
            String out = stringTokenizer.nextToken().trim();
            bufferedWriter.write(out);
            bufferedWriter.write("\n");
          }
        } else {
          throw new RuntimeException("File already exists in the hdfs, remove it from hdfs");
        }
      } else if ("local".equals(fileSystem)) {
        File file = new File(fileName);
        if (!file.exists()) {
          //file.createNewFile();
          bufferedWriter = new BufferedWriter(new FileWriter(fileName));
          while (stringTokenizer.hasMoreTokens()) {
            bufferedWriter.write(stringTokenizer.nextToken().trim());
            bufferedWriter.write("\n");
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        bufferedWriter.flush();
        bufferedWriter.close();
        if (dataOutputStream != null) {
          dataOutputStream.close();
        }
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

