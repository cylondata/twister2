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
package edu.iu.dsc.tws.tset.fn.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSinkFunc;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowFileWriter;

/**
 * This class is responsible for calling the twister2 arrow file writer class
 * and add the data to the list in the arrow file writer and commit the data.
 * @param <T>
 */
public class ArrowBasedSinkFunction<T> extends BaseSinkFunc<Iterator<T>> implements Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSinkFunction.class.getName());

  private final String filePath;
  private final String fileName;
  private final String arrowSchema;

  // transient because this will be created by the prepare method
  private transient Twister2ArrowFileWriter twister2ArrowFileWriter;

  public ArrowBasedSinkFunction(String filepath, String filename, String schema) {
    this.filePath = filepath;
    this.arrowSchema = schema;
    this.fileName = filename;
  }

  @Override
  public void prepare(TSetContext context) {
    super.prepare(context);
    // creating the file writer in the prepare method because, each worker would need to create
    // their own writer
    String filename = this.filePath + "/" + context.getWorkerId() + "/" + this.fileName;
    this.twister2ArrowFileWriter = new Twister2ArrowFileWriter(filename, false,
        this.arrowSchema, context.getParallelism());
    try {
      twister2ArrowFileWriter.setUpTwister2ArrowWrite(context.getWorkerId());
    } catch (Exception e) {
      throw new RuntimeException("Unable to setup arrow file", e);
    }
  }

  @Override
  public void close() {
    if (twister2ArrowFileWriter != null) {
      twister2ArrowFileWriter.close();
    }
  }

  //now this can handle any incoming data from the IteratorLinks
  @Override
  public boolean add(Iterator<T> value) {
    try {
      while (value.hasNext()) {
        twister2ArrowFileWriter.queueArrowData(value.next());
      }
      twister2ArrowFileWriter.commitArrowData();
    } catch (Exception e) {
      throw new RuntimeException("Unable to write arrow file", e);
    }
    return true;
  }
}
