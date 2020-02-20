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
package edu.iu.dsc.tws.tset.fn.impl;

import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class CSVBasedSourceFunction<T> extends TextBasedSourceFunction<String[]> {

  private static final Logger LOG = Logger.getLogger(TextBasedSourceFunction.class.getName());

  private DataSource<String, FileInputSplit<String>> dataSource;
  //private InputSplit<String> dataSplit;
  private TSetContext ctx;

  private String datainputDirectory;
  private int dataSize;
  private int parallel;
  private int count = 0;
  private String partitionerType;

  public CSVBasedSourceFunction(String dataInputdirectory, int datasize,
                                int parallelism, String type) {
    super(dataInputdirectory, datasize, parallelism, type);
    this.datainputDirectory = dataInputdirectory;
    this.dataSize = datasize;
    this.parallel = parallelism;
    this.partitionerType = type;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
  }

  @Override
  public String next() {
    String obj = String.valueOf(super.next());
    Pattern pattern = Pattern.compile(CSVInputSplit.DEFAULT_FIELD_DELIMITER);
    String[] object = pattern.split(obj);
    return String.valueOf(object);

    /*try {
      String obj = String.valueOf(super.next());
      Pattern pattern = Pattern.compile(CSVInputSplit.DEFAULT_FIELD_DELIMITER);
      String[] object = pattern.split(obj);
      return object;
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }*/
  }
}
