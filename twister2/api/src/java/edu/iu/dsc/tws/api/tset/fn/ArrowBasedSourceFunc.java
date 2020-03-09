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
package edu.iu.dsc.tws.api.tset.fn;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.api.formatters.LocalCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteCSVInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class ArrowBasedSourceFunc extends BaseSourceFunc<String> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private Schema arrowSchema;
  private RootAllocator rootAllocator = null;

  private DataSource<String, FileInputSplit<String>> dataSource;
  private InputSplit<String> dataSplit;
  private TSetContext ctx;

  private int dataSize = 100;
  private int parallel;
  private int count = 0;

  private String arrowInputFile;
  private String datainputDirectory;
  private String partitionerType;
  private String message;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallelism, Schema schema, String msg) {
    this.arrowInputFile = arrowinputFile;
    this.parallel = parallelism;
    this.arrowSchema = schema;
    this.partitionerType = msg;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    Config cfg = ctx.getConfig();

    LOG.info("Config values:" + cfg);
    if ("complete".equals(partitionerType)) {
      this.dataSource = new DataSource(cfg, new LocalCompleteCSVInputPartitioner(
          new Path(datainputDirectory), context.getParallelism(), dataSize, cfg), parallel);
    } else {
      this.dataSource = new DataSource(cfg, new LocalCSVInputPartitioner(
          new Path(datainputDirectory), parallel, dataSize, cfg), parallel);
    }
    this.dataSplit = this.dataSource.getNextSplit(context.getIndex());

    FileInputStream fileInputStream;
    try {
      fileInputStream = new FileInputStream(new File(arrowInputFile));
      LOG.info("File Input Stream:" + fileInputStream.getChannel());
      DictionaryProvider.MapDictionaryProvider provider
          = new DictionaryProvider.MapDictionaryProvider();
      ArrowFileReader arrowFileReader = new ArrowFileReader(
          new SeekableReadChannel(fileInputStream.getChannel()), this.rootAllocator);
      VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
      LOG.info(String.format("File size : %d schema is %s", arrowInputFile.length(),
          root.getSchema().toString()));
      List<ArrowBlock> arrowBlockList = arrowFileReader.getRecordBlocks();
      LOG.info("arrow block size:" + arrowBlockList.size());
      arrowFileReader.close();
      fileInputStream.close();
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (IOException ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  @Override
  public boolean hasNext() {
    //return true;
    try {
      if (dataSplit == null || dataSplit.reachedEnd()) {
        dataSplit = dataSource.getNextSplit(getTSetContext().getIndex());
      }
      return dataSplit != null && !dataSplit.reachedEnd();
    } catch (IOException e) {
      throw new RuntimeException("Unable read data split", e);
    }
  }

  @Override
  public String next() {
    //return new String[]{"hello", "hello1"};
    try {
      return dataSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("unable to read arrow file", e);
    }
  }
}
