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
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.data.FSDataInputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class Twister2ArrowFileReader implements ITwister2ArrowFileReader, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowFileReader.class.getName());

  private String arrowInputFile;
  private String arrowSchema;

  private int currentBlock = 0;
  private IntVector intVector;

  private FileInputStream fileInputStream;
  private FSDataInputStream fsDataInputStream;

  private FileSystem fileSystem;

  private RootAllocator rootAllocator;
  private VectorSchemaRoot root;
  private ArrowFileReader arrowFileReader;
  private List<ArrowBlock> arrowBlocks;

  public Twister2ArrowFileReader(String inputFile, String schema) {
    this.arrowInputFile = inputFile;
    this.arrowSchema = schema;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  public void initInputFile() {
    try {
      LOG.info("arrow schema:" + Schema.fromJSON(arrowSchema));
      Path path = new Path(arrowInputFile);
      this.fileSystem = FileSystemUtils.get(path);
      this.fsDataInputStream = fileSystem.open(path);
      this.fileInputStream = new FileInputStream(arrowInputFile);
      this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(
          fileInputStream.getChannel()), rootAllocator);
      this.root = arrowFileReader.getVectorSchemaRoot();
      arrowBlocks = arrowFileReader.getRecordBlocks();
      LOG.info("\nReading the arrow file : " + arrowInputFile
          + "\tFile size:" + arrowInputFile.length()
          + "\tschema:" + root.getSchema().toString()
          + "\tArrow Blocks Size: " + arrowBlocks.size());
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (Exception ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

//  public IntVector getIntegerVector() {
//    try {
//      if (currentBlock < arrowBlocks.size()) {
//        arrowFileReader.loadRecordBatch(arrowBlocks.get(currentBlock++));
//        //it should be generic
//        intVector = (IntVector) root.getFieldVectors().get(0);
//      } else {
//        intVector = null;
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    if (intVector != null) {
//      LOG.info("%%% Count Block:" + currentBlock + "%%% Int Vector:%%%" + intVector);
//    }
//    return intVector;
//  }

  public IntVector getIntegerVector() {
    try {
      if (currentBlock < arrowBlocks.size()) {
        arrowFileReader.loadRecordBatch(arrowBlocks.get(currentBlock++));
        List<FieldVector> fieldVectorList = root.getFieldVectors();
        for (int i = 0; i < fieldVectorList.size(); i++) {
          FieldVector fieldVector = fieldVectorList.get(i);
          if (fieldVector.getMinorType().equals(Types.MinorType.INT)) {
            intVector = getIntegerVector(fieldVector);
          } else if (fieldVector.getMinorType().equals(Types.MinorType.FLOAT4)) { //check this
            getDoubleVector(fieldVector);
          } else if (fieldVector.getMinorType().equals(Types.MinorType.BIGINT)) { //check this
            getBigIntegerVector(fieldVector);
          } else {
            throw new RuntimeException("Not Supported Datatypes Now");
          }
        }
      } else {
        intVector = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return intVector;
  }

  public IntVector getIntegerVector(FieldVector vector) {
    IntVector intVector1 = null;
    try {
      intVector1 = (IntVector) root.getFieldVectors().get(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return intVector1;
  }

  public BigIntVector getBigIntegerVector(FieldVector vector) {
    BigIntVector bigIntVector = null;
    try {
      bigIntVector = (BigIntVector) root.getFieldVectors().get(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return bigIntVector;
  }

  private Float4Vector getDoubleVector(FieldVector vector) {
    return null;
  }
}
