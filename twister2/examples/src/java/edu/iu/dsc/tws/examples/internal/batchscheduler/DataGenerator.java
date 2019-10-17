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

package edu.iu.dsc.tws.examples.internal.batchscheduler;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.lang3.RandomStringUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class DataGenerator {

  private static final Logger LOG = Logger.getLogger(DataGenerator.class.getName());

  private Config config;
  private int workerId;

  public DataGenerator(Config cfg, int workerid) {
    this.config = cfg;
    this.workerId = workerid;
  }

  public DataGenerator(Config cfg) {
    this.config = cfg;
  }

  private static String generatePoints(int numPoints, int dimension, int seedValue) {
    StringBuilder datapoints = new StringBuilder();
    Random r = new Random(seedValue);
    for (int i = 0; i < numPoints; i++) {
      StringBuilder line = new StringBuilder();
      for (int j = 0; j < dimension; j++) {
        double randomValue = r.nextDouble();
        line.append(String.format("%1$,.8f", randomValue));
        if (j < dimension - 1) {
          line.append(",").append("\t");
        }
      }
      datapoints.append(line);
//      if (i < numPoints - 1) {
      datapoints.append("\n");
//      }
    }
    return datapoints.toString();
  }

  private static String generateRandom(int length) {
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }

  public void generate(Path directory, int size, int dimension) {
    try {
      FileSystem fs = FileSystemUtils.get(directory.toUri(), config);
      if (fs.exists(directory)) {
        fs.delete(directory, true);
      }
      FSDataOutputStream outputStream = fs.create(new Path(directory,
          generateRandom(10) + ".txt"));
      PrintWriter pw = new PrintWriter(outputStream);
      String points = generatePoints(size, dimension, 100);
      pw.print(points);
      outputStream.sync();
      pw.close();
    } catch (IOException e) {
      throw new RuntimeException("Data Generation Error Occured", e);
    }

  }
}
