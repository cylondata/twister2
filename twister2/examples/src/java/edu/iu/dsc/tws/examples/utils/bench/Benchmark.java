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
package edu.iu.dsc.tws.examples.utils.bench;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Benchmark {

  private final String benchName;
  private final File csvFile;
  private final HashMap<String, String> colValMap = new HashMap<>();

  public Benchmark(String benchName, String csvFileName) {
    this.benchName = benchName;
    this.csvFile = new File(csvFileName);
  }

  public void writeToCSV() throws IOException {
    //go through the existing columns of csv and determine the order
    List<String> existingHeaderOrder = new ArrayList<>();
    String headerLine = "";
    if (csvFile.exists()) {
      BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFile));
      headerLine = bufferedReader.readLine();
      bufferedReader.close();
      if (headerLine != null) {
        existingHeaderOrder.addAll(Arrays.asList(headerLine.split(",")));
      }
    }
    this.writeInOrder(existingHeaderOrder, headerLine);
  }

  private void writeInOrder(List<String> headerOrder, String headerLine) throws IOException {
    StringBuilder newRow = new StringBuilder();
    for (String header : headerOrder) {
      String value = colValMap.remove(header);
      if (value == null) {
        value = "NULL";
      }
      newRow.append(value).append(',');
    }
    StringBuilder headerLineBuilder = new StringBuilder(headerLine);
    for (String header : colValMap.keySet()) {
      headerLineBuilder.append(header).append(",");
      newRow.append(colValMap.get(header)).append(',');
    }


    boolean newFile = "".equals(headerLine);

    List<String> existingLines = new ArrayList<>();
    if (!newFile) {
      BufferedReader br = new BufferedReader(new FileReader(this.csvFile));
      String line = br.readLine();
      while (line != null) {
        existingLines.add(line);
        line = br.readLine();
      }
      br.close();
    }
    existingLines.add(newRow.toString());

    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(this.csvFile));
    String newHeader = headerLineBuilder.toString();
    bufferedWriter.write(newHeader);
    bufferedWriter.newLine();

    if (newFile) {
      bufferedWriter.write(newRow.toString());
      bufferedWriter.newLine();
    } else if (!newHeader.equals(headerLine)) {
      List<String> lines = new ArrayList<>();
      BufferedReader br = new BufferedReader(new FileReader(this.csvFile));
      String line = br.readLine();
      while (line != null) {
        lines.add(line);
        line = br.readLine();
      }
      br.close();

      lines.set(0, newHeader);
    } else {
      //todo
    }
  }
}
