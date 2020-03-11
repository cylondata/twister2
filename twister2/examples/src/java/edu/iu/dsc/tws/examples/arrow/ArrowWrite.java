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
package edu.iu.dsc.tws.examples.arrow;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowWrite {

  private static final Logger LOG = Logger.getLogger(ArrowWrite.class.getName());

  private String arrowFile;
  private int entries;
  private int maxEntries;
  private long checkSum;
  private long nullEntries;

  private boolean useNullValues;
  private boolean flag;

  private Random random;

  private FileOutputStream fileOutputStream;

  private RootAllocator rootAllocator = null;
  private VectorSchemaRoot root;
  private ArrowFileWriter arrowFileWriter;

  private Twister2ArrowOutputStream twister2ArrowOutputStream;

  public ArrowWrite(String arrowfile, boolean flag) throws FileNotFoundException {
    this.maxEntries = 1024;
    this.checkSum = 0;
    random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.arrowFile = arrowfile;
    this.flag = flag;
    this.fileOutputStream = new FileOutputStream(arrowFile);
  }

  public ArrowWrite(String arrowfile) throws FileNotFoundException {
    this.maxEntries = 1024;
    this.checkSum = 0;
    random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  private Schema makeSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    return new Schema(childrenBuilder.build(), null);
  }

  public void callTwister2ArrowWrite() throws Exception {
    twister2ArrowOutputStream.writeData();
  }

  public void arrowFileWrite() throws Exception {
    //this.fileOutputStream = new FileOutputStream(arrowFile);
    this.twister2ArrowOutputStream = new Twister2ArrowOutputStream(
        this.arrowFileWriter, fileOutputStream);
    Schema schema = makeSchema();
    this.root = VectorSchemaRoot.create(schema, this.rootAllocator);
    DictionaryProvider.MapDictionaryProvider provider
        = new DictionaryProvider.MapDictionaryProvider();
    if (!flag) {
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          this.fileOutputStream.getChannel());
    } else {
      this.arrowFileWriter = new ArrowFileWriter(root, provider, this.twister2ArrowOutputStream);
    }
    if (twister2ArrowOutputStream != null) {
      LOG.info("Twister2 output stream:" + twister2ArrowOutputStream.toString());
    }
  }
}
