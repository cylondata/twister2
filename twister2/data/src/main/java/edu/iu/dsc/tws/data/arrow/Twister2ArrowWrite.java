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
package edu.iu.dsc.tws.data.arrow;

import java.io.FileOutputStream;
import java.util.Random;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class Twister2ArrowWrite {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowWrite.class.getName());

  private String arrowFile;
  private int entries;
  private int maxEntries = 1024;
  private long checkSum;
  private long nullEntries;

  private int batchSize;
  private boolean useNullValues;
  private boolean flag;

  private Random random;

  private transient FileOutputStream fileOutputStream;

  private RootAllocator rootAllocator = null;
  private transient VectorSchemaRoot root;
  private ArrowFileWriter arrowFileWriter;

  private transient Twister2ArrowOutputStream twister2ArrowOutputStream;
  private ArrowGenerator[] data;

  public Twister2ArrowWrite(String arrowfile, boolean flag) {
    this.maxEntries = 1024;
    this.checkSum = 0;
    this.batchSize = 100;
    this.random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.arrowFile = arrowfile;

    this.flag = flag;
    this.data = new ArrowGenerator[this.entries];
    for (int i = 0; i < this.entries; i++) {
      this.data[i] = new ArrowGenerator(this.random);
    }
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  private Schema makeSchema() {
    ImmutableList.Builder<Field> builder = ImmutableList.builder();
    builder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    return new Schema(builder.build(), null);
  }

  public void setUpTwister2ArrowWrite() throws Exception {
    this.fileOutputStream = new FileOutputStream(arrowFile);
    Schema schema = makeSchema();
    this.root = VectorSchemaRoot.create(schema, this.rootAllocator);
    LOG.info("root values" + this.root);
    DictionaryProvider.MapDictionaryProvider provider
        = new DictionaryProvider.MapDictionaryProvider();
    if (!flag) {
      LOG.info("I am inside if loop:");
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          this.fileOutputStream.getChannel());
    } else {
      twister2ArrowOutputStream = new Twister2ArrowOutputStream(this.fileOutputStream);
      this.arrowFileWriter = new ArrowFileWriter(root, provider, twister2ArrowOutputStream);
    }
    if (twister2ArrowOutputStream != null) {
      LOG.info("Twister2 output stream:" + twister2ArrowOutputStream.toString());
    }

    if (false) {
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        LOG.info("vector values:" + vector);
      }
    }
    writeArrowData();
  }

  public void writeArrowData() throws Exception {
    this.batchSize = 100;
    arrowFileWriter.start();
    for (int i = 0; i < this.entries;) {
      int toProcessItems = Math.min(this.batchSize, this.entries - i);
      root.setRowCount(toProcessItems);
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        writeFieldInt(vector, i, toProcessItems);
      }
      arrowFileWriter.writeBatch();
      i += toProcessItems;
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
      intVector.setSafe(i, isSet(), this.data[from + i].randomInt);
    }
    fieldVector.setValueCount(items);
  }

  private int isSet() {
    if (useNullValues) {
      this.nullEntries++;
      return 0;
    }
    return 1;
  }

  private class ArrowGenerator {

    private int randomInt;
    private Random random;

    protected ArrowGenerator(Random random) {
      this.random = random;
      randomInt = random.nextInt(1024);
    }
  }
}
