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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;

public class Twister2ArrowOutputStream implements WritableByteChannel {

  private long bytesSoFar;
  private int batchSize;
  private int entries;
  private int nullEntries;
  private Boolean isOpen;
  private boolean useNullValues;
  private byte[] tempBuffer;

  private Random random;
  private FileOutputStream fileOutputStream;

  private VectorSchemaRoot root;
  private ArrowFileWriter arrowFileWriter;

  public Twister2ArrowOutputStream(FileOutputStream fileoutputStream) {
    this.useNullValues = false;
    this.nullEntries = 0;
    this.fileOutputStream = fileoutputStream;
    this.isOpen = true;
    this.tempBuffer = new byte[1024 * 1024];
    this.bytesSoFar = 0;
  }

  @Override
  public int write(ByteBuffer byteBuffer) throws IOException {
    int remaining = byteBuffer.remaining();
    int soFar = 0;
    while (soFar < remaining) {
      int toPush = Math.min(remaining - soFar, this.tempBuffer.length);
      byteBuffer.get(this.tempBuffer, 0, toPush);
      this.fileOutputStream.write(this.tempBuffer, 0, toPush);
      soFar += toPush;
    }
    this.bytesSoFar += remaining;
    return remaining;
  }

  @Override
  public boolean isOpen() {
    return this.isOpen;
  }

  @Override
  public void close() throws IOException {
    this.fileOutputStream.close();
    this.isOpen = false;
  }

  public void writeData() throws Exception {
    this.batchSize = 100;
    arrowFileWriter.start();
    for (int i = 0; i < 1; i++) {
      int toProcessItems = Math.min(this.batchSize, this.entries - i);
      root.setRowCount(toProcessItems);
      for (Field field : root.getSchema().getFields()) {
        FieldVector vector = root.getVector(field.getName());
        switch (vector.getMinorType()) {
          case INT:
            writeFieldInt(vector, i, toProcessItems);
            break;
          default:
            throw new Exception(" Not supported yet type: " + vector.getMinorType());
        }
      }
      arrowFileWriter.writeBatch();
      //i += toProcessItems;
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
//    for (int i = 0; i < items; i++) {
//      intVector.setSafe(i, isSet(), this.data[from + i].anInt);
//    }
    fieldVector.setValueCount(items);
  }

  private int isSet() {
    if (useNullValues) {
      //if (this.random.nextInt() % 10 == 0) {
      this.nullEntries++;
      return 0;
      //}
    }
    return 1;
  }
}
