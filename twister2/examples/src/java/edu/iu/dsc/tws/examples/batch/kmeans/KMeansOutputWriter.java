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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

public class KMeansOutputWriter {

  protected KMeansOutputWriter() {
  }

  /**
   * This method write the centroids into the filesystem or HDFS which is based on the user
   * submitted value.
   */
  public static void writeToOutputFile(double[][] finalValue, String fileName,
                                       Config config, String fileSystem) {
    BufferedWriter bufferedWriter = null;
    HadoopFileSystem hadoopFileSystem = null;
    HadoopDataOutputStream dataOutputStream = null;

    try {
      if ("hdfs".equals(fileSystem)) {
        HdfsUtils hdfsUtils = new HdfsUtils(config, fileName);
        hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
        Path path = hdfsUtils.getPath();

        if (hadoopFileSystem.exists(path)) {
          hadoopFileSystem.delete(path, false);
        }
        dataOutputStream = hadoopFileSystem.create(path);
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"));
        for (int i = 0; i < finalValue.length; i++) {
          for (int j = 0; j < finalValue[0].length; j++) {
            bufferedWriter.write(finalValue[i][j] + "\t");
          }
          bufferedWriter.write("\n");
        }
      } else if ("local".equals(fileSystem)) {
        File file = new File(fileName);
        if (file.exists()) {
          file.delete();
        }
        bufferedWriter = new BufferedWriter(new FileWriter(fileName));
        for (int i = 0; i < finalValue.length; i++) {
          for (int j = 0; j < finalValue[0].length; j++) {
            bufferedWriter.write(finalValue[i][j] + "\t");
          }
          bufferedWriter.write("\n");
        }

      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to write centroids file", e);
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
        throw new RuntimeException("Failed to close file", e);
      }
    }
  }
}
