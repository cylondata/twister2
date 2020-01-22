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
package edu.iu.dsc.tws.tset.sets.batch;

import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.out.CSVOutputWriter;
import edu.iu.dsc.tws.data.api.out.FileOutputWriter;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.tset.sinks.FileSink;

public class FileTSet<T> extends FileSink {

  private CSVOutputWriter csvOutputWriter;

  public FileTSet(FileOutputWriter out) {
    super(out);
  }

  public FileSink<T> writeAsCSV(String filepath) {
    return writeAsCSV(filepath, CSVInputSplit.DEFAULT_LINE_DELIMITER,
        CSVInputSplit.DEFAULT_FIELD_DELIMITER);
  }

  private FileSink<T> writeAsCSV(String filepath, String lineDelimiter, String fieldDelimiter) {
    return internalWriteAsCSV(new Path(filepath),  lineDelimiter, fieldDelimiter);
  }

  private FileSink<T> internalWriteAsCSV(Path path, String lineDelimiter, String fieldDelimiter) {
    csvOutputWriter = new CSVOutputWriter(FileSystem.WriteMode.OVERWRITE, path);
    csvOutputWriter.setWriteMode(null);
    return output(csvOutputWriter);
  }

  private FileSink<T> output(CSVOutputWriter csvoutputWriter) {
    return new FileSink(csvoutputWriter);
  }

  public boolean writeAsCSV(CachedTSet<T> centers) {
    return new FileSink<>(csvOutputWriter).add(String.valueOf(centers));
  }
}
