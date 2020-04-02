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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class Twister2ArrowFileWriter implements ITwister2ArrowFileWriter, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowFileWriter.class.getName());

  private String arrowFile;

  private int totalitems = 0;
  private int totaldataValues;

  private int entries;
  private int maxEntries;
  private int batchSize;
  private int[] data;

  private long checkSum;
  private long nullEntries;

  private boolean useNullValues;
  private boolean flag;

  private Random random;

  private Twister2ArrowOutputStream twister2ArrowOutputStream;
  private FileOutputStream fileOutputStream;

  private transient RootAllocator rootAllocator;
  private VectorSchemaRoot root;
  private ArrowFileWriter arrowFileWriter;
  private String arrowSchema;

  public Twister2ArrowFileWriter(String arrowfile, boolean flag) {
    this.maxEntries = 1024;
    this.checkSum = 0;
    this.batchSize = 100;
    this.random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.data = new int[this.entries];
    for (int i = 0; i < this.entries; i++) {
      this.data[i] = this.random.nextInt(1024);
    }
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  public Twister2ArrowFileWriter(String arrowfile, boolean flag, String schema) {
    this.maxEntries = 1024;
    this.checkSum = 0;
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.arrowSchema = schema;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);

    this.batchSize = 100;
    this.random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.arrowFile = arrowfile;
    this.data = new int[this.entries];
    for (int i = 0; i < this.entries; i++) {
      this.data[i] = this.random.nextInt(1024);
    }
    LOG.info("constructor getting called" + arrowSchema);
  }

  public boolean setUpTwister2ArrowWrite(int workerId) throws Exception {
//    try {
//      this.arrowSchema = Schema.fromJSON(schema);
//    } catch (IOException ioe) {
//      throw new RuntimeException("IOException Occured", ioe);
//    }
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.root = VectorSchemaRoot.create(Schema.fromJSON(arrowSchema), this.rootAllocator);
    File file = new File(arrowFile/* + workerId*/);
    if (file.exists()) {
      file.delete();
    }
    LOG.info("file name:" + file.getName());
    this.fileOutputStream = new FileOutputStream(file);
    DictionaryProvider.MapDictionaryProvider provider
        = new DictionaryProvider.MapDictionaryProvider();
    if (!flag) {
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          this.fileOutputStream.getChannel());
    } else {
      this.twister2ArrowOutputStream = new Twister2ArrowOutputStream(this.fileOutputStream);
      this.arrowFileWriter = new ArrowFileWriter(root, provider, this.twister2ArrowOutputStream);
    }
    //writeArrowData();
    return true;
  }

  public void writeArrowData(Integer integerdata) throws Exception {
    arrowFileWriter.start();
    int i = 0;
    for (Field field : root.getSchema().getFields()) {
      FieldVector fieldVector = root.getVector(field.getName());
      IntVector intVector = (IntVector) fieldVector;
      intVector.setInitialCapacity(100);
      intVector.allocateNew();
      intVector.setSafe(totalitems, isSet(), integerdata);
      fieldVector.setValueCount(totalitems);
      totalitems++;
    }
    root.setRowCount(totaldataValues);
    arrowFileWriter.writeBatch();
  }

  public void writeArrowData() throws Exception {
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
      intVector.setSafe(i, isSet(), this.data[from + i]);
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

  public void close() {
    try {
      arrowFileWriter.end();
      arrowFileWriter.close();
      fileOutputStream.flush();
      fileOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
