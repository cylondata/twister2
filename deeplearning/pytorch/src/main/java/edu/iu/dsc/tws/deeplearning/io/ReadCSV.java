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

package edu.iu.dsc.tws.deeplearning.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ReadCSV {

  private String filePath = "";
  private int samples = 1;
  private int features = 1;
  private int partitionId = 0;
  private int samplesPerPartition = 0;
  private int starterPoint = 0;
  private int parallelism = 4;

  public ReadCSV(String filePath, int samples, int features, int partitionId, int parallelism) {
    this.filePath = filePath;
    this.samples = samples;
    this.features = features;
    this.partitionId = partitionId;
    this.parallelism = parallelism;
  }

  public void read() {

    if (Files.exists(Paths.get(this.filePath))) {
      System.out.println("File Exists");
      try {
        String currentLine = "";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(this.filePath));
        int lines = 0;
        while ((currentLine = bufferedReader.readLine()) != null) {
          lines++;
        }
        System.out.println("Data Points : "  + lines);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      System.out.println("File doesn't exist : " + this.filePath);
    }
  }

  private void calculateMeta() {

  }

  @Override
  public String toString() {
    return "ReadCSV{"
        + "filePath='" + filePath
        + '\''
        + ", samples=" + samples
        + ", features=" + features
        + ", partitionId=" + partitionId
        + ", samplesPerPartition=" + samplesPerPartition
        + ", starterPoint=" + starterPoint
        + ", parallelism=" + parallelism
        + '}';
  }
}
