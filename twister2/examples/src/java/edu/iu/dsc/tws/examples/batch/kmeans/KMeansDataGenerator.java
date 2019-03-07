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
import java.io.PrintWriter;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.lang3.RandomStringUtils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FSDataOutputStream;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

/**
 * Generate a data set
 * <p>
 * 1. We can generate in each worker
 * 2. We can generate in a common location shared by workers, such as HDFS or NFS
 */
public final class KMeansDataGenerator {

  private static final Logger LOG = Logger.getLogger(KMeansDataGenerator.class.getName());

  private KMeansDataGenerator() {
  }

  /**
   * Generate a data set
   *
   * @param type type of file, i.e. csv, text, binary
   * @param directory the directory to generate
   * @param numOfFiles number of files to create
   * @param sizeOfFile size of each file, different types have a different meaning
   * @param sizeMargin size will be varied about this much
   */
  public static void generateData(String type, Path directory, int numOfFiles, int sizeOfFile,
                                  int sizeMargin, int dimension, Config cfg)
      throws IOException {
    if ("csv".equals(type)) {
      generateCSV(directory, numOfFiles, sizeOfFile, sizeMargin, dimension, cfg);
    } else if ("txt".equals(type)) {
      generateText(directory, numOfFiles, sizeOfFile, sizeMargin, dimension, cfg);
    } else {
      throw new RuntimeException("Unsupported data gen type: " + type);
    }
  }

  private static void generateText(Path directory, int numOfFiles, int sizeOfFile,
                                   int sizeMargin, int dimension, Config config)
      throws IOException {
    FileSystem fs = FileSystem.get(directory.toUri(), config);
    if (fs.exists(directory)) {
      fs.delete(directory, true);
    }
    for (int i = 0; i < numOfFiles; i++) {
      FSDataOutputStream outputStream = fs.create(new Path(directory,
          generateRandom(10) + ".txt"));
      PrintWriter pw = new PrintWriter(outputStream);
      String points = generatePoints(sizeOfFile, dimension, sizeMargin);
      pw.print(points);
      pw.close();
    }
  }

  private static String generatePoints(int numPoints, int dimension, int seedValue) {
    StringBuilder datapoints = new StringBuilder();
    Random r = new Random(seedValue);
    for (int i = 0; i < numPoints; i++) {
      StringBuilder line = new StringBuilder();
      for (int j = 0; j < dimension; j++) {
        double randomValue = r.nextDouble();
        line.append(randomValue);
        if (j == 0) {
          line.append(",").append("\t");
        }
      }
      datapoints.append(line).append("\n");
    }
    return datapoints.toString();
  }


  /**
   * Generate a random csv file, we generate a csv with 10 attributes
   *
   * @param directory the path of the directory
   */
  private static void generateCSV(Path directory, int numOfFiles, int sizeOfFile,
                                  int sizeMargin, int dimension, Config config) throws IOException {

    FileSystem fs = FileSystem.get(directory.toUri(), config);
    if (fs.exists(directory)) {
      fs.delete(directory, true);
    }
    for (int i = 0; i < numOfFiles; i++) {
      FSDataOutputStream outputStream = fs.create(new Path(directory,
          generateRandom(10) + ".csv"));
      PrintWriter pw = new PrintWriter(outputStream);
      String points = generatePoints(sizeOfFile, dimension, sizeMargin);
      pw.print(points);
      pw.close();
    }
  }

  private static String generateCSVLine(int fields) {
    StringBuilder row = new StringBuilder();
    for (int i = 0; i < fields - 1; i++) {
      row.append(generateRandom(4)).append(", ");
    }
    row.append(generateRandom(4));
    return row.toString();
  }

  private static String generateRandom(int length) {
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }
}
