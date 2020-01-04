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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class CSVOutputWriter extends FileOutputWriter<String> {

  private static final Logger LOG = Logger.getLogger(FileOutputWriter.class.getName());

  protected static String lineDelimiter = CSVInputSplit.DEFAULT_LINE_DELIMITER;

  protected static String fieldDelimiter = CSVInputSplit.DEFAULT_FIELD_DELIMITER;

  /**
   * File system object
   */
  protected FileSystem fs;

  /**
   * Opened streams
   */
  protected Map<Integer, FSDataOutputStream> openStreams = new HashMap<>();

  /**
   * Write mode of the files
   */
  protected FileSystem.WriteMode writeMode;

  /**
   * File output path
   */
  protected Path outPath;

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outputPath) {
    this(writeMode, outputPath, lineDelimiter, fieldDelimiter);
  }

//  @Override
//  protected void createOutput(int partition, FSDataOutputStream out) {
//  }
//
//  protected FSDataOutputStream fsOut;
//
//  private boolean quoteStrings = false;
//
//  protected void writeRecord(int partition, String data) {
//    //int numberOfFields = data.getNumberOfFields();
//    /*int numebrOfFields = 2;
//    for (int i = 0; i < numebrOfFields; i++) {
//      //Object obj = data.getField(i);
//      Object obj = null;
//      if (obj != null) {
//        if (i != 0) {
//          this.fsOut.write(fieldDelimiter);
//        }
//
//        if (quoteStrings) {
//          if (obj instanceof String) {
//            try {
//              this.fsOut.write('"');
//              this.fsOut.write(obj.toString());
//              this.fsOut.write('"');
//            } catch (IOException e) {
//              e.printStackTrace();
//            }
//          } else {
//            this.fsOut.write(obj.toString());
//          }
//        }
//      } else {
//        if (i != 0) {
//          this.fsOut.write(fieldDelimiter);
//        }
//      }
//    }
//    this.fsOut.write(lineDelimiter);*/
//  }

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outputPath,
                         String lineDelimiter, String fieldDelimiter) {
    super(writeMode, outputPath);

    if (lineDelimiter == null) {
      throw new IllegalArgumentException("line delimiter is null");
    }

    if (fieldDelimiter == null) {
      throw new IllegalArgumentException("field delimiter is null");
    }

    this.lineDelimiter = lineDelimiter;
    this.fieldDelimiter = fieldDelimiter;

    try {
      this.fs = FileSystemUtils.get(outPath.toUri());
      //this.fsOut = fs.create(outPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create file system for : " + outPath.toUri());
    }
  }


  @Override
  public void configure(Config config) {
  }

  private Map<Integer, PrintWriter> writerMap = new HashMap<>();

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
