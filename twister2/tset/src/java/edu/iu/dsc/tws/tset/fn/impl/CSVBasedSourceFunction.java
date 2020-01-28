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
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;

public class CSVBasedSourceFunction<T> extends BaseSourceFunc<T> {

  private static final Logger LOG = Logger.getLogger(CSVBasedSourceFunction.class.getName());

  private DataSource<T, FileInputSplit<T>> dataSource;
  private InputSplit<T> dataSplit;
  private TSetContext ctx;

  private String datainputDirectory;
  private int dataSize;
  private int parallel;

  public CSVBasedSourceFunction(String dataInputdirectory) {
    this.datainputDirectory = dataInputdirectory;
  }

  public CSVBasedSourceFunction(String dataInputdirectory, int datasize) {
    this.datainputDirectory = dataInputdirectory;
    this.dataSize = datasize;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    Config cfg = ctx.getConfig();

    this.dataSize = cfg.getIntegerValue(DataObjectConstants.DSIZE, 100);
    this.parallel = cfg.getIntegerValue(DataObjectConstants.PARALLELISM_VALUE, 4);
    this.dataSource = new DataSource(cfg, new LocalCSVInputPartitioner(new Path(datainputDirectory),
        parallel, dataSize, cfg), parallel);
    this.dataSplit = this.dataSource.getNextSplit(context.getIndex());
    LOG.info("%%%% Task Index Value:" + context.getIndex() + "\tDataSource:\t" + this.dataSource);
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

  private int count = 0;
  @Override
  public T next() {
    try {
      T object = dataSplit.nextRecord(null);
      count++;
      LOG.fine("count value:" + count);
      return object;
      //return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }
}
