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

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowFileReader;

//TODO: Check Chunk Arrays for parallelism > 1
//TODO: LOOK AT ARROW METADATA check the chunk array and split it into different workers
public class ArrowBasedSourceFunc extends BaseSourceFunc<Integer> implements Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSourceFunc.class.getName());

  private int parallel;

  private String arrowInputFile;

  private Twister2ArrowFileReader twister2ArrowFileReader;

  private transient Schema arrowSchema;

  public ArrowBasedSourceFunc(String arrowinputFile, int parallelism, Schema schema) {
    this.arrowInputFile = arrowinputFile;
    this.parallel = parallelism;
    this.arrowSchema = schema;
    this.twister2ArrowFileReader = new Twister2ArrowFileReader(arrowinputFile, schema);
  }

  /**
   * Prepare method
   */
  public void prepare(TSetContext context) {
    super.prepare(context);
    this.twister2ArrowFileReader.processInputFile();
  }

  private IntVector intVector = null;
  private int currentCell = 0;

  @Override
  public boolean hasNext() {
    try {
      if (intVector == null || currentCell == intVector.getValueCount()) {
        intVector = twister2ArrowFileReader.getIntegerVector();
        currentCell = 0;
      }
      return intVector != null && currentCell < intVector.getValueCount();
    } catch (Exception e) {
      throw new RuntimeException("Unable to read int vector", e);
    }
  }

  @Override
  public Integer next() {
    return intVector.get(currentCell++); // post increment current cell
  }
}
