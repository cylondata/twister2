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
package edu.iu.dsc.tws.examples.batch.arrow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataInputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.DataFileReader;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

public final class ArrowDataFileWriter {

  private static final Logger LOG = Logger.getLogger(DataFileReader.class.getName());

  private final Config config;
  private final String fileName;

  private int entries;
  private int maxEntries;
  private int batchSize;
  private long checksum;
  private long nullEntries;
  private boolean useNullValues;

  private ArrowExampleClass[] data;
  private RootAllocator rootAllocator = null;
  private VectorSchemaRoot root;
  private FileOutputStream fileOutputStream;
  private ArrowFileWriter arrowFileWriter;

  private Random random;

  private volatile FSDataInputStream fdis;

  public ArrowDataFileWriter(Config cfg, String filename) {
    this.config = cfg;
    this.fileName = filename;
    this.batchSize = 100;
    this.checksum = 0;
    this.maxEntries = 1024;
    random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    for (int i = 0; i < this.entries; i++) {
      this.data[i] = new ArrowExampleClass(this.random, i);
      long csum = this.data[i].getSumHash();
      checksum += csum;
    }
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  /**
   * It reads the datapoints from the corresponding file and store the data in a two-dimensional
   * array for the later processing. The size of the two-dimensional array should be equal to the
   * number of clusters and the dimension considered for the clustering process.
   */
  public void setUpwriteArrowFile(Path path, boolean flag) {
    try {
      final FileSystem fs = FileSystemUtils.get(path, config);
      this.fileOutputStream = new FileOutputStream(new File(fileName));
      Schema schema = makeSchema();
      this.root = VectorSchemaRoot.create(schema, this.rootAllocator);
      DictionaryProvider.MapDictionaryProvider provider
          = new DictionaryProvider.MapDictionaryProvider();

      if (flag) {
        this.arrowFileWriter = new ArrowFileWriter(
            root, provider, this.fileOutputStream.getChannel());
      } /*else {
        this.arrowFileWriter = new ArrowFileWriter(root, provider,
        new ArrowOutputStream(this.fileOutputStream));
      }*/
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  private int isSet() {
    if (useNullValues) {
      if (this.random.nextInt(10) == 0) {
        this.nullEntries++;
        return 0;
      }
    }
    return 1;
  }

  public void writeToArrowFile() throws Exception {
    arrowFileWriter.start();
    for (int i = 0; i < this.entries; i++) {
      int toProcessItems = Math.min(this.batchSize, this.entries - i);
      // set the batch row count
      root.setRowCount(toProcessItems);
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        switch (vector.getMinorType()) {
          case INT:
            writeFieldInt(vector, i, toProcessItems);
            break;
          case BIGINT:
            writeFieldLong(vector, i, toProcessItems);
            break;
          default:
            throw new Exception(" Not supported yet type: " + vector.getMinorType());
        }
      }
    }
    arrowFileWriter.end();
    arrowFileWriter.close();
    fileOutputStream.flush();
    fileOutputStream.close();
  }

  private void writeFieldInt(FieldVector fieldVector, int from, int items) {
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(items);
    intVector.allocateNew();
    for (int i = 0; i < items; i++) {
      intVector.setSafe(i, isSet(), this.data[from + i].anInt);
    }
    fieldVector.setValueCount(items);
  }

  private void writeFieldLong(FieldVector fieldVector, int from, int items) {
    BigIntVector bigIntVector = (BigIntVector) fieldVector;
    bigIntVector.setInitialCapacity(items);
    bigIntVector.allocateNew();
    for (int i = 0; i < items; i++) {
      bigIntVector.setSafe(i, isSet(), this.data[from + i].aLong);
    }
    bigIntVector.setValueCount(items);
  }

  private Schema makeSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    childrenBuilder.add(new Field("long", FieldType.nullable(new ArrowType.Int(64, true)), null));
    childrenBuilder.add(new Field("binary", FieldType.nullable(new ArrowType.Binary()), null));
    childrenBuilder.add(new Field("double", FieldType.nullable(new ArrowType.FloatingPoint(SINGLE)),
        null));
    return new Schema(childrenBuilder.build(), null);
  }

  private long showColumnSum() {
    long intSum = 0;
    long longSum = 0;
    long arrSum = 0;
    long floatSum = 0;
    for (int i = 0; i < this.entries; i++) {
      intSum += this.data[i].anInt;
      longSum += this.data[i].aLong;
      arrSum += ArrowExampleClass.hashArray(this.data[i].arr);
      floatSum += this.data[i].aFloat;
    }
    System.out.println("intSum " + intSum + " longSum " + longSum + " arrSum " + arrSum
        + " floatSum " + floatSum);
    return intSum + longSum + arrSum + floatSum;
  }
}
