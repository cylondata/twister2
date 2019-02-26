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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.iu.dsc.tws.common.config.Config;

public class BenchmarkResultsRecorder {

  private static final Logger LOG = Logger.getLogger(BenchmarkResultsRecorder.class.getName());

  private String benchName;
  private File csvFile;
  private final HashMap<String, String> colValMap = new HashMap<>();
  private boolean accept;

  public BenchmarkResultsRecorder(Config config, boolean acceptCondition) {
    this.accept = acceptCondition && config.getBooleanValue(
        BenchmarkMetadata.ARG_RUN_BENCHMARK, false
    );
    if (this.accept) {
      //parse config to get metadata
      String metadataB64 = config.getStringValue(BenchmarkMetadata.ARG_BENCHMARK_METADATA);
      byte[] decodedMeta = Base64.getDecoder().decode(metadataB64);
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        BenchmarkMetadata benchmarkMetadata = objectMapper.readValue(
            decodedMeta, BenchmarkMetadata.class
        );
        this.csvFile = new File(benchmarkMetadata.getResultsFile());
        this.benchName = benchmarkMetadata.getId();
        benchmarkMetadata.getArgs().forEach(
            arg -> recordColumn(arg.getColumn(), config.get(arg.getArg()))
        );
      } catch (IOException e) {
        this.accept = false;
        LOG.log(Level.SEVERE, "Couldn't initialize results recorder", e);
      }
    }
  }

  public void recordColumn(String column, Object value) {
    if (!this.accept) {
      return;
    }
    this.colValMap.put(column, value.toString().replace(",", "/"));
  }

  public void writeToCSV() {
    if (!this.accept) {
      return;
    }
    try {
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
      LOG.info("Wrote results to " + this.csvFile.getAbsolutePath());
    } catch (IOException ioex) {
      LOG.log(Level.SEVERE, "Couldn't write results to the file", ioex);
      throw new RuntimeException("Error in writing results to the file", ioex);
    }
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
      boolean headerCaptured = false;
      while (line != null) {
        if (headerCaptured) { //omit the header in existing file
          existingLines.add(line);
        }
        line = br.readLine();
        headerCaptured = true;
      }
      br.close();
    }
    existingLines.add(newRow.toString());

    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(this.csvFile));
    String newHeader = headerLineBuilder.toString();
    bufferedWriter.write(newHeader);
    bufferedWriter.newLine();

    for (String existingLine : existingLines) {
      bufferedWriter.write(existingLine);
      bufferedWriter.newLine();
    }
    bufferedWriter.close();
  }
}
