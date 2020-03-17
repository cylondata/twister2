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
import org.apache.arrow.vector.types.Types;
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

  private VectorSchemaRoot root;

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
      ArrowFileReader arrowFileReader = new ArrowFileReader(
          new SeekableReadChannel(fileInputStream.getChannel()), this.rootAllocator);
      //VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
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
      }

      //TODO: Check Chunk Arrays for parallelism > 1
      //TODO: LOOK AT ARROW METADATA check the chunk array and split it into different workers
      List<FieldVector> fieldVectors = root.getFieldVectors();
      for (int j = 0; j < fieldVectors.size(); j++) {
        Types.MinorType mt = fieldVectors.get(j).getMinorType();
        try {
          switch (mt) {
            case INT:
              showIntAccessor(fieldVectors.get(j));
              break;
            default:
              throw new Exception("minortype");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      arrowFileReader.close();
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (IOException ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  private List<FieldVector> fieldVector;

  private FieldVector fVector;

  @Override
  public boolean hasNext() {
    //TODO: move the Show Int logic into has next to next
    fieldVector = root.getFieldVectors();
    int j = 0;
    while (j < fieldVector.size()) {
      Types.MinorType mt = fieldVector.get(j).getMinorType();
      j++;
      fVector = fieldVector.get(j);
    }
    return true;
  }

  @Override
  public Integer next() {
    IntVector intVector = (IntVector) fVector;
    int value = 0;
    for (int j = 0; j < intVector.getValueCount(); j++) {
      if (!intVector.isNull(j)) {
        value = intVector.get(j);
      }
    }
    return value;
  }


  private void showIntAccessor(FieldVector fx) {
    IntVector intVector = (IntVector) fx;
    for (int j = 0; j < intVector.getValueCount(); j++) {
      if (!intVector.isNull(j)) {
        int value = intVector.get(j);
        LOG.info("\t\t intAccessor[" + j + "] " + value);
        intCsum += value;
        this.checkSumx += value;
      } else {
        this.nullEntries++;
        LOG.info("\t\t intAccessor[" + j + "] : NULL ");
      }
    }
  }

  public static long hashArray(byte[] data) {
    long ret = 0;
    for (int i = 0; i < data.length; i++) {
      ret += data[i];
    }
    return ret;
  }
}
