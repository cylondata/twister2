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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class Twister2ArrowFileReader implements ITwister2ArrowFileReader, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowFileReader.class.getName());

  private String arrowInputFile;

  private transient FileInputStream fileInputStream;

  private transient Schema arrowSchema;
  private transient RootAllocator rootAllocator = null;

  private transient VectorSchemaRoot root;
  private transient ArrowFileReader arrowFileReader;
  private List<ArrowBlock> arrowBlocks;

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
      arrowBlocks = arrowFileReader.getRecordBlocks();
      Schema schema = root.getSchema();
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

  private IntVector intVector;
  private int currentValue = 0;
  private int currentBlock = 0;

  public IntVector getIntegerVector() {
    try {
      if (currentBlock < arrowBlocks.size()) {
        arrowFileReader.loadRecordBatch(arrowBlocks.get(currentBlock++));
        intVector = (IntVector) root.getFieldVectors().get(0);
      } else {
        intVector = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (intVector != null) {
      LOG.info("%%% Count Block:" + currentBlock + "%%% Int Vector:%%%" + intVector);
    }
    return intVector;
  }
}
