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
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowFileWriter;

public class ArrowBasedSinkFunc<T> implements Serializable, SinkFunc<Iterator<Integer>> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSinkFunc.class.getName());

  private String arrowfileName = null;

  private int parallel;

  private TSetContext ctx;
  private transient Schema schema;
  private Twister2ArrowFileWriter twister2ArrowFileWriter;

  public ArrowBasedSinkFunc(String filepath, int parallelism, String arrowSchema) {
    this.parallel = parallelism;
    this.arrowfileName = filepath;
    this.twister2ArrowFileWriter = new Twister2ArrowFileWriter(
        arrowfileName, true, arrowSchema);
  }

  @Override
  public void prepare(TSetContext context) {
    this.ctx = context;
    try {
      twister2ArrowFileWriter.setUpTwister2ArrowWrite(ctx.getWorkerId());
    } catch (Exception e) {
      throw new RuntimeException("Unable to setup arrow file", e);
    }
  }

  @Override
  public void close() {
    twister2ArrowFileWriter.close();
  }

  @Override
  public DataPartition<?> get() {
    return null;
  }

  @Override
  public boolean add(Iterator<Integer> value) {
    LOG.info("add function getting called:" + value);
    try {
      while (value.hasNext()) {
        twister2ArrowFileWriter.writeArrowData(value.next().intValue());
      }
      /*if (value == null) {
        twister2ArrowFileWriter.close();
      }*/
    } catch (Exception e) {
      throw new RuntimeException("Unable to write arrow file", e);
    }
    return true;
  }
}
