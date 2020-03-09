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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
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

public class ArrowBasedSourceFunc extends BaseSourceFunc<String> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private Schema arrowSchema;
  private transient RootAllocator rootAllocator = null;

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

  private long checkSumx;
  private long intCsum;
  private long longCsum;
  private long arrCsum;
  private long floatCsum;
  private long nullEntries;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallelism, Schema schema) {
    this.arrowInputFile = arrowinputFile;
    this.parallel = parallelism;
    this.arrowSchema = schema;
    this.checkSumx = 0;
    this.intCsum = 0;
    this.longCsum = 0;
    this.arrCsum = 0;
    this.floatCsum = 0;
  }

  /**
   * Prepare method
   *
   * @param context
   */
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.ctx = context;
    Config cfg = ctx.getConfig();
    FileInputStream fileInputStream;
    try {
      fileInputStream = new FileInputStream(new File(arrowInputFile));
      LOG.info("File Input Stream:" + fileInputStream.getChannel());
      DictionaryProvider.MapDictionaryProvider provider
          = new DictionaryProvider.MapDictionaryProvider();
      ArrowFileReader arrowFileReader = new ArrowFileReader(
          new SeekableReadChannel(fileInputStream.getChannel()), this.rootAllocator);
      VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();

      LOG.info(String.format("File size : %d schema is %s",
          arrowInputFile.length(), root.getSchema().toString()));

      List<ArrowBlock> arrowBlockList = arrowFileReader.getRecordBlocks();
      LOG.info("arrow block size:" + arrowBlockList.size());
      for (int i = 0; i < arrowBlockList.size(); i++) {
        ArrowBlock rbBlock = arrowBlockList.get(i);
      }

      List<FieldVector> fieldVector = root.getFieldVectors();
      for (int j = 0; j < fieldVector.size(); j++) {
        Types.MinorType mt = fieldVector.get(j).getMinorType();
        try {
          switch (mt) {
            case INT:
              showIntAccessor(fieldVector.get(j));
              break;
            case BIGINT:
              showBigIntAccessor(fieldVector.get(j));
              break;
            case VARBINARY:
              showVarBinaryAccessor(fieldVector.get(j));
              break;
            case FLOAT4:
              showFloat4Accessor(fieldVector.get(j));
              break;
            case FLOAT8:
              showFloat8Accessor(fieldVector.get(j));
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

  private void showIntAccessor(FieldVector fx) {
    IntVector intVector = (IntVector) fx;
    for (int j = 0; j < intVector.getValueCount(); j++) {
      if (!intVector.isNull(j)) {
        int value = intVector.get(j);
        System.out.println("\t\t intAccessor[" + j + "] " + value);
        intCsum += value;
        this.checkSumx += value;
      } else {
        this.nullEntries++;
        System.out.println("\t\t intAccessor[" + j + "] : NULL ");
      }
    }
  }

  private void showBigIntAccessor(FieldVector fx) {
    BigIntVector bigIntVector = (BigIntVector) fx;
    for (int j = 0; j < bigIntVector.getValueCount(); j++) {
      if (!bigIntVector.isNull(j)) {
        long value = bigIntVector.get(j);
        System.out.println("\t\t bigIntAccessor[" + j + "] " + value);
        longCsum += value;
        this.checkSumx += value;
      } else {
        this.nullEntries++;
        System.out.println("\t\t bigIntAccessor[" + j + "] : NULL ");
      }
    }
  }

  private void showVarBinaryAccessor(FieldVector fx) {
    VarBinaryVector varBinaryVector = (VarBinaryVector) fx;
    for (int j = 0; j < varBinaryVector.getValueCount(); j++) {
      if (!varBinaryVector.isNull(j)) {
        byte[] value = varBinaryVector.get(j);
        long valHash = hashArray(value);
        arrCsum += valHash;
        this.checkSumx += valHash;
      } else {
        this.nullEntries++;
        System.out.println("\t\t varBinaryAccessor[" + j + "] : NULL ");
      }
    }
  }

  private void showFloat4Accessor(FieldVector fx) {
    Float4Vector float4Vector = (Float4Vector) fx;
    for (int j = 0; j < float4Vector.getValueCount(); j++) {
      if (!float4Vector.isNull(j)) {
        float value = float4Vector.get(j);
        System.out.println("\t\t float4[" + j + "] " + value);
        floatCsum += value;
        this.checkSumx += value;
      } else {
        this.nullEntries++;
        System.out.println("\t\t float4[" + j + "] : NULL ");
      }
    }
  }

  private void showFloat8Accessor(FieldVector fx) {
    Float8Vector float8Vector = (Float8Vector) fx;
    for (int j = 0; j < float8Vector.getValueCount(); j++) {
      if (!float8Vector.isNull(j)) {
        double value = float8Vector.get(j);
        System.out.println("\t\t float8[" + j + "] " + value);
        floatCsum += value;
        this.checkSumx += value;
      } else {
        this.nullEntries++;
        System.out.println("\t\t float8[" + j + "] : NULL ");
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

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public String next() {
    return "hello";
  }
}
