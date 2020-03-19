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
import java.io.Serializable;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;

public class ArrowBasedSourceFunc extends BaseSourceFunc<Integer> implements Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private transient Schema arrowSchema;
  private transient RootAllocator rootAllocator = null;

  private DataSource<String, FileInputSplit<String>> dataSource;
  private InputSplit<String> dataSplit;
  private TSetContext ctx;

  private int parallel;

  private String arrowInputFile;

  private long checkSumx;
  private long intCsum;
  private long nullEntries;

  private List<FieldVector> fieldVector;
  private FieldVector fVector;

  private VectorSchemaRoot root;

  private ArrowFileReader arrowFileReader;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallelism, Schema schema) {
    this.arrowInputFile = arrowinputFile;
    this.parallel = parallelism;
    this.arrowSchema = schema;
    this.checkSumx = 0;
    this.intCsum = 0;
  }

  /**
   * Prepare method
   */
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.ctx = context;
    Config cfg = ctx.getConfig();

    FileInputStream fileInputStream;
    try {
      fileInputStream = new FileInputStream(new File(arrowInputFile));
      DictionaryProvider.MapDictionaryProvider provider
          = new DictionaryProvider.MapDictionaryProvider();
      arrowFileReader = new ArrowFileReader(new SeekableReadChannel(
          fileInputStream.getChannel()), this.rootAllocator);
      root = arrowFileReader.getVectorSchemaRoot();
      LOG.info(String.format("File size : %d schema is %s",
          arrowInputFile.length(), root.getSchema().toString()));
      List<ArrowBlock> arrowBlockList = arrowFileReader.getRecordBlocks();
      LOG.info("Number of arrow blocks:" + arrowBlockList.size());
      for (int i = 0; i < arrowBlockList.size(); i++) {
        ArrowBlock rbBlock = arrowBlockList.get(i);
        LOG.info("\t[" + i + "] ArrowBlock, offset: " + rbBlock.getOffset()
            + ", metadataLength: " + rbBlock.getMetadataLength()
            + ", bodyLength " + rbBlock.getBodyLength());
        LOG.info("\t[" + i + "] row count for this block is " + root.getRowCount());
      }

      //TODO: Check Chunk Arrays for parallelism > 1
      //TODO: LOOK AT ARROW METADATA check the chunk array and split it into different workers
      //arrowFileReader.close();
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (IOException ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  @Override
  public boolean hasNext() {
    if (root.getFieldVectors() != null) {
      fieldVector = root.getFieldVectors();
    }
    return true;
  }

  @Override
  public Integer next() {
    int i = 0;
    int value = 0;
    while (i < fieldVector.size()) {
      //Types.MinorType mt = fieldVector.get(i).getMinorType();
      fVector = fieldVector.get(i);
      IntVector intVector = (IntVector) fVector;
      for (int j = 0; j < intVector.getValueCount(); j++) {
        if (!intVector.isNull(j)) {
          value = intVector.get(j);
        }
      }
      i++;
    }
    //todo: we have to check this
    try {
      arrowFileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return value;
  }
}
