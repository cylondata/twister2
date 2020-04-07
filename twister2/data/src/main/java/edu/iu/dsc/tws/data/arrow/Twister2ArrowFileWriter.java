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
import java.util.ArrayList;
import java.util.List;
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
  private transient VectorSchemaRoot root;
  private transient ArrowFileWriter arrowFileWriter;
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
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.arrowSchema = schema;
    this.batchSize = 1000;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.arrowFile = arrowfile;
    LOG.info("constructor getting called" + arrowSchema);
  }

  public boolean setUpTwister2ArrowWrite(int workerId) throws Exception {
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.root = VectorSchemaRoot.create(Schema.fromJSON(arrowSchema), this.rootAllocator);
    File file = new File(arrowFile/* + workerId*/);
    if (file.exists()) {
      file.delete();
    }
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
    return true;
  }

  @Override
  public void writeArrowData() throws Exception {
  }

  private List<Integer> integersList = new ArrayList<>();

  public void writeArrowData(Integer integerdata) {
    integersList.add(integerdata);
  }

  public void processArrowData() throws Exception {
    arrowFileWriter.start();
    for (int i = 0; i < integersList.size();) {
      int toProcessItems = Math.min(this.batchSize, this.integersList.size() - i);
      LOG.info("to process items:" + toProcessItems);
      root.setRowCount(toProcessItems);
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        writeFieldInt(vector, i, toProcessItems);
      }
      arrowFileWriter.writeBatch();
      i += toProcessItems;
    }
  }

  private void writeFieldInt(FieldVector fieldVector, int from, int items) {
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(items);
    intVector.allocateNew();
    for (int i = 0; i < items; i++) {
      intVector.setSafe(i, isSet(), this.integersList.get(from + i));
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

  //  public void writeArrowData(Integer integerdata) throws Exception {
//    LOG.info("schema value:" + root.getSchema());
//    arrowFileWriter.start();
//    root.setRowCount(100);
//    for (Field field : root.getSchema().getFields()) {
//      int i = 0;
//      FieldVector fieldVector = root.getVector(field.getName());
//      LOG.info("field vector:" + fieldVector);
//      IntVector intVector = (IntVector) fieldVector;
//      intVector.setInitialCapacity(100);
//      intVector.allocateNew();
//      intVector.setSafe(i, isSet(), integerdata);
//      LOG.info("INT VECTOR:" + intVector);
//      fieldVector.setValueCount(100);
//      totalitems++;
//      i++;
//    }
//    arrowFileWriter.writeBatch();
//  }
}
