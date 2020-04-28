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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  private int parallel;

  private List<Object> dataList = new ArrayList<>();

  private boolean useNullValues;
  private boolean flag;

  private Twister2ArrowOutputStream twister2ArrowOutputStream;
  private FSDataOutputStream fsDataOutputStream;

  private FileSystem fileSystem;

  private transient RootAllocator rootAllocator;
  private transient VectorSchemaRoot root;
  private transient ArrowFileWriter arrowFileWriter;

  private Twister2ArrowVectorGenerator twister2ArrowVectorGenerator;
  private final Map<FieldVector, Generator> generatorMap = new LinkedHashMap<>();


  // todo lets give a meaningful name for this flag variable
  public Twister2ArrowFileWriter(String arrowfile, boolean flag, String schema,
                                 int parallelism) {
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.arrowSchema = schema;
    this.batchSize = 1000;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.parallel = parallelism;
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

    LOG.info("root schema fields:" + root.getSchema().getFields());
    for (Field field : root.getSchema().getFields()) {
      FieldVector vector = root.getVector(field.getName());
      if (vector.getMinorType().equals(Types.MinorType.INT)) {
//        twister2ArrowVectorGenerator = new Twister2ArrowVectorGenerator(root, dataList);
        this.generatorMap.put(vector, new IntVectorGenerator());
      } else if (vector.getMinorType().equals(Types.MinorType.BIGINT)) {
//        twister2ArrowVectorGenerator = new Twister2ArrowVectorGenerator(root, dataList);
        this.generatorMap.put(vector, new BigIntVectorGenerator());
      } else if (vector.getMinorType().equals(Types.MinorType.FLOAT4)) {
        this.generatorMap.put(vector, new FloatVectorGenerator());
      } else {
        throw new RuntimeException("unsupported arrow write type");
      }
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
//      for (Field field : root.getSchema().getFields()) {
//        FieldVector vector = root.getVector(field.getName());
//        twister2ArrowVectorGenerator.vectorGeneration(vector, i, min);
//      }

      for (Map.Entry<FieldVector, Generator> e : generatorMap.entrySet()) {
        e.getValue().generate(e.getKey(), i, min, 1);
      }

      arrowFileWriter.writeBatch();
      i += min;
    }
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


  private interface Generator {
    <T extends FieldVector> void generate(T vector, int from, int items, int isSet);
  }

  private class IntVectorGenerator implements Generator {
    @Override
    public <T extends FieldVector> void generate(T intVector1, int from, int items, int isSet) {
      IntVector intVector = (IntVector) intVector1;
      intVector.setInitialCapacity(items);
      intVector.allocateNew();
      for (int i = 0; i < items; i++) {
        intVector.setSafe(i, isSet, (int) dataList.get(from + i));
      }
      intVector.setValueCount(items);
    }
  }

  private class BigIntVectorGenerator implements Generator {
    @Override
    public <T extends FieldVector> void generate(T bigIntVector1, int from, int items, int isSet) {
      BigIntVector bigIntVector = (BigIntVector) bigIntVector1;
      bigIntVector.setInitialCapacity(items);
      bigIntVector.allocateNew();
      for (int i = 0; i < items; i++) {
        Long l = new Long(dataList.get(from + i).toString());
        bigIntVector.setSafe(i, isSet, l);
      }
      bigIntVector.setValueCount(items);
    }
  }

  private class FloatVectorGenerator implements Generator {
    @Override
    public <T extends FieldVector> void generate(T floatVector1, int from, int items, int isSet) {
      Float4Vector floatVector = (Float4Vector) floatVector1;
      floatVector.setInitialCapacity(items);
      floatVector.allocateNew();
      for (int i = 0; i < items; i++) {
        floatVector.setSafe(i, isSet, (float) dataList.get(from + i));
      }
      floatVector.setValueCount(items);
    }
  }
}
