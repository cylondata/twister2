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
package edu.iu.dsc.tws.tset.sources;

import java.io.IOException;

import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class CSVSource extends BaseSourceFunc<String> {

  private DataSource<String, FileInputSplit<String>> dataSource;
  private InputSplit<String> dataSplit;

  private String datainputDirectory;

  public CSVSource(String dataInputdirectory) {
    this.datainputDirectory = dataInputdirectory;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.dataSource = new DataSource(context.getConfig(), new LocalCSVInputPartitioner(
        new Path(datainputDirectory), context.getParallelism(), context.getConfig()), 100);
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
  public String next() {
    try {
      return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split!");
    }
  }
}
