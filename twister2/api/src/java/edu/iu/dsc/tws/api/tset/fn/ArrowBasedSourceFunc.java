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
package edu.iu.dsc.tws.api.tset.fn;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowFileReader;

//TODO: Check Chunk Arrays for parallelism > 1
//TODO: LOOK AT ARROW METADATA check the chunk array and split it into different workers
public class ArrowBasedSourceFunc extends BaseSourceFunc<Integer> implements Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private TSetContext ctx;

  private int parallel;

  private String arrowInputFile;

  private transient Schema arrowSchema;
  private transient RootAllocator rootAllocator;

  private List<FieldVector> fieldVector;
  private List<ArrowBlock> arrowBlockList;

  private FieldVector fVector;

  private transient FileInputStream fileInputStream;
  private Twister2ArrowFileReader twister2ArrowFileReader;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallelism) {
    this.arrowInputFile = arrowinputFile;
    this.parallel = parallelism;
  }

  /**
   * Prepare method
   */
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.ctx = context;
    Config cfg = ctx.getConfig();
    twister2ArrowFileReader = new Twister2ArrowFileReader(arrowInputFile);
  }

  @Override
  public boolean hasNext() {
    //twister2ArrowFileReader.processInputFile();
    fieldVector = twister2ArrowFileReader.getFieldVector();
    return fieldVector != null;
  }

  @Override
  public Integer next() {
    int value = processIntData(fieldVector);
    return value;
  }

  private Integer processIntData(List<FieldVector> fieldVectorList) {
    int value = 0;
    for (int i = 0; i < fieldVectorList.size(); i++) {
      IntVector intVector = (IntVector) fieldVectorList.get(i);
      for (int j = 0; j < intVector.getValueCount(); j++) {
        if (!intVector.isNull(j)) {
          value = intVector.get(j);
        }
      }
    }
    return value;
  }
}
