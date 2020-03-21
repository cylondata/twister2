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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class Twister2ArrowFileReader implements SeekableByteChannel, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowFileReader.class.getName());

  private String arrowInputFile;

  private boolean isOpen;

  private transient FileInputStream fileInputStream;

  private FieldVector fVector;
  private List<FieldVector> fieldVector;

  private transient Schema arrowSchema;
  private transient RootAllocator rootAllocator = null;

  private transient VectorSchemaRoot root;
  private transient ArrowFileReader arrowFileReader;

  public Twister2ArrowFileReader(String inputFile) {
    this.arrowInputFile = inputFile;
  }

  public void processInputFile() {
    try {
      this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
      this.fileInputStream = new FileInputStream(arrowInputFile);
      this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(
          fileInputStream.getChannel()), rootAllocator);
      this.root = arrowFileReader.getVectorSchemaRoot();
      List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
      LOG.info("\nReading the arrow file : " + arrowInputFile
          + "\tFile size:" + arrowInputFile.length()
          + "\tschema:" + root.getSchema().toString()
          + "\tArrow Blocks Size: " + arrowBlocks.size());
      for (int i = 0; i < arrowBlocks.size(); i++) {
        ArrowBlock rbBlock = arrowBlocks.get(i);
        arrowFileReader.loadRecordBatch(rbBlock);
        this.setFieldVector(root.getFieldVectors());
        LOG.info("\t[" + i + "] row count for this block is " + root.getRowCount());
      }
      this.close();
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (Exception ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  public List<FieldVector> getFieldVector() {
    processInputFile();
    LOG.info("field vector:" + fieldVector.size() + "\t" + fieldVector);
    return fieldVector;
  }

  public void setFieldVector(List<FieldVector> fieldVector) {
    this.fieldVector = fieldVector;
  }

  /*public List<FieldVector> getFieldVectorList() {
    for (int j = 0; j < fieldVector.size(); j++) {
      IntVector intVector = (IntVector) fieldVector.get(j);
      for (int k = 0; k < intVector.getValueCount(); k++) {
        if (!intVector.isNull(k)) {
          int value = intVector.get(k);
          LOG.info("\t\t intAccessor[" + k + "] " + value);
        } else {
          LOG.info("\t\t intAccessor[" + k + "] : NULL ");
        }
      }
    }
    return fieldVector;
  }*/


  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    return 0;
  }

  @Override
  public int write(ByteBuffer byteBuffer) throws IOException {
    return 0;
  }

  @Override
  public long position() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel position(long l) throws IOException {
    return null;
  }

  @Override
  public long size() throws IOException {
    return 0;
  }

  @Override
  public SeekableByteChannel truncate(long l) throws IOException {
    return null;
  }

  @Override
  public boolean isOpen() {
    return this.isOpen;
  }

  @Override
  public void close() throws IOException {
    this.fileInputStream.close();
    this.isOpen = false;
  }
}
