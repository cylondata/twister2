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

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class TextBasedSourceFunction<T> extends BaseSourceFunc<T> {

  private static final Logger LOG = Logger.getLogger(TextBasedSourceFunction.class.getName());

  private DataSource<T, FileInputSplit<T>> dataSource;
  private InputSplit<T> dataSplit;
  private TSetContext ctx;

  private String datainputDirectory;
  private int dataSize;
  private int parallel;
  private int count = 0;
  private String partitionerType;

  public TextBasedSourceFunction() {
  }

  public TextBasedSourceFunction(String dataInputdirectory, int datasize,
                                 int parallelism, String type) {
    this.datainputDirectory = dataInputdirectory;
    this.dataSize = datasize;
    this.parallel = parallelism;
    this.partitionerType = type;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    Config cfg = ctx.getConfig();
    if ("complete".equals(partitionerType)) {
      this.dataSource = new DataSource(cfg, new LocalCompleteCSVInputPartitioner(
          new Path(datainputDirectory), context.getParallelism(), dataSize, cfg), parallel);
    } else {
      this.dataSource = new DataSource(cfg, new LocalCSVInputPartitioner(
          new Path(datainputDirectory), parallel, dataSize, cfg), parallel);
    }
    this.dataSplit = this.dataSource.getNextSplit(context.getIndex());
  }

  @Override
  public boolean hasNext() {
    try {
      if (dataSplit == null || dataSplit.reachedEnd()) {
        dataSplit = dataSource.getNextSplit(getTSetContext().getIndex());
      }
      return dataSplit != null && !dataSplit.reachedEnd();
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }

  @Override
  public T next() {
    try {
      T object = dataSplit.nextRecord(null);
      return object;
      //return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }
}
