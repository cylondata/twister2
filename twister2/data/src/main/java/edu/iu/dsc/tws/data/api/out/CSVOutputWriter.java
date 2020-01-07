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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;

public class CSVOutputWriter extends FileOutputWriter<String> {

  private static final Logger LOG = Logger.getLogger(CSVOutputWriter.class.getName());

  protected static String lineDelimiter = CSVInputSplit.DEFAULT_LINE_DELIMITER;
  protected static String fieldDelimiter = CSVInputSplit.DEFAULT_FIELD_DELIMITER;
  protected static String tabDelimiter = CSVInputSplit.DEFAULT_TAB_DELIMITER;

  private Map<Integer, PrintWriter> writerMap = new HashMap<>();

  private boolean allowedNullValues = true;

  private boolean quoteStrings = false;

  private String charsetName;

  private transient Charset charset;

  private String[] headers;

  private Path path;

  private FSDataOutputStream outputStream;

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath, Config config) {
    this(writeMode, outPath, lineDelimiter, fieldDelimiter, tabDelimiter, config, "UTF-8");
  }

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath, String linedelimiter,
                         String fielddelimiter, String tabdelimiter, Config config,
                         String charset) {
    super(writeMode, outPath);

    if (linedelimiter == null) {
      throw new Twister2RuntimeException("line delimiter shouldn't be null");
    }

    if (fielddelimiter == null) {
      throw new Twister2RuntimeException("field delimiter should'nt be null");
    }

    this.fieldDelimiter = fielddelimiter;
    this.lineDelimiter = linedelimiter;
    this.tabDelimiter = tabdelimiter;
    /*this.charset = Charset.forName(charsetName);*/
    this.allowedNullValues = false;
    this.path = outPath;
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

  public void setHeaders(String[] headerNames) {
    this.headers = headerNames;
  }

  @Override
  public void writeRecord(int partition, String data) {
    if (writerMap.containsKey(partition)) {
      writerMap.get(partition).println(data);
    }
  }

  @Override
  protected void writeRecord(FSDataOutputStream out, String data) {

  }

  public void createOutput() {
    try {
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      outputStream = fs.create(new Path(path, generateRandom(10) + ".csv"));
      pw = new PrintWriter(outputStream);
    } catch (IOException e) {
      throw new RuntimeException("IOException Occured");
    }
  }

  public void write(FSDataOutputStream out, String data) {
    pw = new PrintWriter(out);
    for (int i = 0; i < headers.length; i++) {
      pw.write(headers[i]);
      if (i < headers.length - 1) {
        pw.write(fieldDelimiter);
        pw.write(tabDelimiter);
      }
    }
    pw.write(lineDelimiter);
    pw.write(data);
  }

  @Override
  public void write(String data) {
    for (int i = 0; i < headers.length; i++) {
      pw.write(headers[i]);
      if (i < headers.length - 1) {
        pw.write(fieldDelimiter);
        pw.write(tabDelimiter);
      }
    }
    pw.write(lineDelimiter);
  }

  public void writeRecord(String data) {
    for (int i = 0; i < headers.length; i++) {
      pw.write(headers[i]);
      if (i < headers.length - 1) {
        pw.write(fieldDelimiter);
        pw.write(tabDelimiter);
      }
    }
    pw.write(lineDelimiter);
    pw.write(data);
  }

  @Override
  public void close() {
    if (!writerMap.isEmpty()) {
      for (PrintWriter pw1 : writerMap.values()) {
        pw1.close();
      }
      writerMap.clear();
    } else {
      pw.close();
    }
    super.close();
  }
}

