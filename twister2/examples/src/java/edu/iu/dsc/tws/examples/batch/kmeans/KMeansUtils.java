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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

public class KMeansUtils {

  private Config config;

  protected KMeansUtils(Config cfg) {
    this.config = cfg;
  }

  private static BufferedReader bufferedReader = null;
  private static BufferedWriter bufferedWriter = null;
  private static HadoopFileSystem hadoopFileSystem = null;
  private static HadoopDataOutputStream dataOutputStream = null;

  public static BufferedReader getBufferedReader(
      Config config, String fileName, String fileSystem) {
    try {
      if ("hdfs".equals(fileSystem)) {
        Path path = getPath(config, fileName);
        if (hadoopFileSystem.exists(path)) {
          bufferedReader = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(path)));
        }
      } else if ("local".equals(fileSystem)) {
        File f = new File(fileName);
        bufferedReader = new BufferedReader(new FileReader(f));
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException("File Not Found:", e);
    } catch (IOException e) {
      throw new RuntimeException("IO Exception occured", e);
    }
    return bufferedReader;
  }

  /**
   * This method is responsible for creating the respective buffered writers which is based on the
   * file system such as local or hdfs.
   */
  public static BufferedWriter getBufferedWriter(Config config, String fileName,
                                                 String fileSystem) {
    try {
      if ("hdfs".equals(fileSystem)) {
        Path path = getPath(config, fileName);
        if (hadoopFileSystem.exists(path)) {
          hadoopFileSystem.delete(path, false);
        }
        dataOutputStream = hadoopFileSystem.create(path);
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
            StandardCharsets.UTF_8));
      } else if ("local".equals(fileSystem)) {
        File file = new File(fileName);
        if (file.exists()) {
          file.delete();
        }
        bufferedWriter = new BufferedWriter(new FileWriter(fileName));
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Buffered Writer Creation Error", ioe);
    }
    return bufferedWriter;
  }

  private static Path getPath(Config config, String fileName) {
    HdfsUtils hdfsUtils = new HdfsUtils(config, fileName);
    hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
    Path path = hdfsUtils.getPath();
    return path;
  }

  public static void writeClose() {
    try {
      if (bufferedWriter != null) {
        bufferedWriter.close();
      }
      if (dataOutputStream != null) {
        dataOutputStream.close();
      }
      if (hadoopFileSystem != null) {
        hadoopFileSystem.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Not able to close", e);
    }
  }

  /**
   * This method is used to calculate the number of lines in the file which uses the line number
   * reader to read the
   */
  public static int getNumberOfLines(String fileName) {
    int noOfLines;
    try {
      File file = new File(fileName);
      LineNumberReader numberReader = new LineNumberReader(new FileReader(file));
      numberReader.skip(Long.MAX_VALUE);
      noOfLines = numberReader.getLineNumber();
      numberReader.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Error reading the line number", ioe);
    }
    return noOfLines;
  }

  /**
   * This method is to write the final centroid value in the output file which is based on the
   * file system (local or hdfs) submitted by the user.
   */
  public static void writeToOutputFile(double[][] finalValue, String fileName,
                                       Config config, String fileSystem) {
    try {
      bufferedWriter = getBufferedWriter(config, fileName, fileSystem);
      for (double[] aFinalValue : finalValue) {
        for (int j = 0; j < finalValue[0].length; j++) {
          bufferedWriter.write(aFinalValue[j] + "\t");
        }
        bufferedWriter.write("\n");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Output File Writing Exception", ioe);
    } finally {
      writeClose();
    }
  }

  public static void readClose() {
    try {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
      if (hadoopFileSystem != null) {
        hadoopFileSystem.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Not able to close", e);
    }
  }
}
