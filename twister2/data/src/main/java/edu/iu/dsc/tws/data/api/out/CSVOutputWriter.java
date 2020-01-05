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
package edu.iu.dsc.tws.data.api.out;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;

public class CSVOutputWriter extends FileOutputWriter<String> {

  private static final Logger LOG = Logger.getLogger(CSVOutputWriter.class.getName());

  protected static String lineDelimiter = CSVInputSplit.DEFAULT_LINE_DELIMITER;

  protected static String fieldDelimiter = CSVInputSplit.DEFAULT_FIELD_DELIMITER;

  private Map<Integer, PrintWriter> writerMap = new HashMap<>();

  private boolean allowedNullValues = true;

  private boolean quoteStrings = false;

  private String charsetName;

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath) {
    this(writeMode, outPath, lineDelimiter, fieldDelimiter);
  }

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath,
                         String linedelimiter, String fielddelimiter) {
    super(writeMode, outPath);

    if (linedelimiter == null) {
      throw new Twister2RuntimeException("line delimiter shouldn't be null");
    }

    if (fielddelimiter == null) {
      throw new Twister2RuntimeException("field delimiter should'nt be null");
    }

    this.fieldDelimiter = fielddelimiter;
    this.lineDelimiter = linedelimiter;
    this.allowedNullValues = false;
  }

  public boolean isAllowedNullValues() {
    return allowedNullValues;
  }

  public void setAllowedNullValues(boolean allowedNullValues) {
    this.allowedNullValues = allowedNullValues;
  }

  public boolean isQuoteStrings() {
    return quoteStrings;
  }

  public void setQuoteStrings(boolean quoteStrings) {
    this.quoteStrings = quoteStrings;
  }

  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }

  @Override
  public void createOutput(int partition, FSDataOutputStream out) {
    if (!writerMap.containsKey(partition)) {
      writerMap.put(partition, new PrintWriter(out));
    }
  }

  @Override
  public void writeRecord(int partition, String data) {
    if (writerMap.containsKey(partition)) {
      writerMap.get(partition).println(data);
    }
  }

  @Override
  public void close() {
    for (PrintWriter pw : writerMap.values()) {
      pw.close();
    }
    writerMap.clear();
    super.close();
  }
}
