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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class Twister2ArrowFileWriter implements ITwister2ArrowFileWriter, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowFileWriter.class.getName());

  private String arrowFile;
  private String arrowSchema;

  private int batchSize;

  private List<Object> dataList = new ArrayList<>();

  private boolean useNullValues;
  private boolean flag;

  private Twister2ArrowOutputStream twister2ArrowOutputStream;
  private FSDataOutputStream fsDataOutputStream;

  private FileSystem fileSystem;

  private transient RootAllocator rootAllocator;
  private transient VectorSchemaRoot root;
  private transient ArrowFileWriter arrowFileWriter;

  // todo lets give a meaningful name for this flag variable
  public Twister2ArrowFileWriter(String arrowfile, boolean flag, String schema) {
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.arrowSchema = schema;
    this.batchSize = 1000;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  public boolean setUpTwister2ArrowWrite(int workerId) throws Exception {
    LOG.fine("%%%%%%%%% worker id details:" + workerId + "\t" + arrowFile);
    this.root = VectorSchemaRoot.create(Schema.fromJSON(arrowSchema), this.rootAllocator);
    Path path = new Path(arrowFile);
    this.fileSystem = FileSystemUtils.get(path);
    this.fsDataOutputStream = fileSystem.create(path);
    this.twister2ArrowOutputStream = new Twister2ArrowOutputStream(this.fsDataOutputStream);
    DictionaryProvider.MapDictionaryProvider provider
        = new DictionaryProvider.MapDictionaryProvider();
    if (!flag) {
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          this.fsDataOutputStream.getChannel());
    } else {
      this.arrowFileWriter = new ArrowFileWriter(root, provider, this.twister2ArrowOutputStream);
    }
    return true;
  }

  public void queueArrowData(Object data) {
    dataList.add(data);
  }

  public void commitArrowData() throws Exception {
    arrowFileWriter.start();
    for (int i = 0; i < dataList.size();) {
      int min = Math.min(this.batchSize, this.dataList.size() - i);
      root.setRowCount(min);
      LOG.info("root schema fields:" + root.getSchema().getFields());
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        if (vector.getMinorType().equals(Types.MinorType.INT)) {
          intVectorGeneration((IntVector) vector, i, min);
        } else if (vector.getMinorType().equals(Types.MinorType.FLOAT4)) {
          doubleVectorGeneration((Float4Vector) vector, i, min);
        } else if (vector.getMinorType().equals(Types.MinorType.LONG)) {
          longVectorGeneration((BigIntVector) vector, i, min);
        } else {
          throw new RuntimeException("Not Supported Datatypes Now");
        }
      }
      arrowFileWriter.writeBatch();
      i += min;
    }
  }

  private void intVectorGeneration(IntVector intVector, int from, int items) {
    intVector.setInitialCapacity(items);
    intVector.allocateNew();
    for (int i = 0; i < items; i++) {
      intVector.setSafe(i, isSet(), (int) this.dataList.get(from + i));
    }
    intVector.setValueCount(items);
    LOG.info("int vector:" + intVector);
  }

  private void doubleVectorGeneration(Float4Vector floatVector, int from, int items) {
    floatVector.setInitialCapacity(items);
    floatVector.allocateNew();
    for (int i = 0; i < items; i++) {
      floatVector.setSafe(i, isSet(), (float) this.dataList.get(from + i));
    }
    floatVector.setValueCount(items);
  }

  private void longVectorGeneration(BigIntVector bigIntVector, int from, int items) {
    bigIntVector.setInitialCapacity(items);
    bigIntVector.allocateNew();
    for (int i = 0; i < items; i++) {
      Long l = new Long(this.dataList.get(from + i).toString());
      bigIntVector.setSafe(i, isSet(), l);
    }
    bigIntVector.setValueCount(items);
  }

  private void writeFieldInt(FieldVector fieldVector, int from, int items) {
    //todo: we need to find a way to write to the field vector without having to cast to this INT
    //type now! because we wouldn't know the type of object during compile time.
    IntVector intVector = (IntVector) fieldVector;
    intVector.setInitialCapacity(items);
    intVector.allocateNew();
    for (int i = 0; i < items; i++) {
      intVector.setSafe(i, isSet(), (int) this.dataList.get(from + i));
    }
    fieldVector.setValueCount(items);
  }

  private int isSet() {
    if (useNullValues) {
      return 0;
    }
    return 1;
  }

  public void close() {
    try {
      arrowFileWriter.end();
      arrowFileWriter.close();
      fsDataOutputStream.flush();
      fsDataOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
