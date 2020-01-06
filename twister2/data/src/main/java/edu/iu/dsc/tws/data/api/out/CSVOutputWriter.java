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

import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class CSVOutputWriter extends FileOutputWriter<String> {

  private static final Logger LOG = Logger.getLogger(CSVOutputWriter.class.getName());

  protected static String lineDelimiter = CSVInputSplit.DEFAULT_LINE_DELIMITER;
  protected static String fieldDelimiter = CSVInputSplit.DEFAULT_FIELD_DELIMITER;

  private Map<Integer, PrintWriter> writerMap = new HashMap<>();

  private boolean allowedNullValues = true;

  private boolean quoteStrings = false;

  private String charsetName;

  private transient Charset charset;

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath) {
    this(writeMode, outPath, lineDelimiter, fieldDelimiter, "UTF-8");
  }

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath,
                         String linedelimiter, String fielddelimiter, String charset) {
    super(writeMode, outPath);

    if (linedelimiter == null) {
      throw new Twister2RuntimeException("line delimiter shouldn't be null");
    }

    if (fielddelimiter == null) {
      throw new Twister2RuntimeException("field delimiter should'nt be null");
    }

    this.fieldDelimiter = fielddelimiter;
    this.lineDelimiter = linedelimiter;
    //this.charset = Charset.forName(charsetName);
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
  protected void createOutput(FSDataOutputStream out) {
    try {
      this.fs = FileSystemUtils.get(outPath.toUri());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String[] headers;

  public void setHeaders(String[] headerNames) {
    this.headers = headerNames;
  }

  @Override
  public void writeRecord(String data) {
    FSDataOutputStream fsOut;
    Path path = new Path(outPath.toString());
    try {
      fsOut = fs.create(path);
      LOG.info("received input data:" + fsOut);
      //printWriter = new PrintWriter(String.valueOf(path));
      //fsOut.write(Integer.parseInt(data));
      //printWriter.write(data);
    } catch (IOException ioe) {
      throw new RuntimeException("IOException Occured");
    }
  }


  public void writeRecord(int partition, String[] data) {
    int numberOfFields = data.length;
    for (int i = 0; i < numberOfFields; i++) {
      Object object = data[i];
      if (object != null) {
        if (i != 0) {
          if (writerMap.containsKey(partition)) {
            writerMap.get(partition).println(data);
          }
        }

        if (quoteStrings) {
          if (object instanceof String) {
            LOG.info("string instance is:" + object);
          }
        }
      }
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
