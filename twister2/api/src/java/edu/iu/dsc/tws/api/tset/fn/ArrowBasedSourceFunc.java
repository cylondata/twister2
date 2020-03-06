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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.fs.io.InputSplit;

public class ArrowBasedSourceFunc extends BaseSourceFunc<String[]> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private String arrowInputFile;
  private int parallelism;
  private Schema arrowSchema;

  private InputSplit<String> dataSplit;
  private RootAllocator rootAllocator = null;
  private TSetContext ctx;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallel, Schema schema)
      throws Exception {
    this.arrowInputFile = arrowinputFile;
    this.parallelism = parallel;
    this.arrowSchema = schema;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    Config cfg = ctx.getConfig();
    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(arrowInputFile);
      ArrowFileReader arrowFileReader = new ArrowFileReader(
          new SeekableReadChannel(fileInputStream.getChannel()), this.rootAllocator);
      VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
      LOG.info("File size : " + arrowInputFile.length()
          + " schema is " + root.getSchema().toString());
      List<ArrowBlock> arrowBlockList = arrowFileReader.getRecordBlocks();
      LOG.info("arrow block size:" + arrowBlockList.size());
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (IOException ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public String[] next() {
    return new String[]{"hello", "hello1"};
//    try {
//      return dataSplit.nextRecord(null);
//    } catch (IOException e) {
//      throw new RuntimeException("unable to read arrow file", e);
//    }
  }
}
