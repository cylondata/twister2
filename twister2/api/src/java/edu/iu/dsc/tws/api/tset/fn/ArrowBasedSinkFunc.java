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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.api.out.FileOutputWriter;

public class ArrowBasedSinkFunc<I> implements Serializable, SinkFunc<Integer> {

  private static final Logger LOG = Logger.getLogger(ArrowBasedSinkFunc.class.getName());

  private FileOutputWriter<Integer> outputWriter;

  private BufferedWriter writer;

  private String arrowfileName = null;

  private int partition;
  private int parallel = 0;

  private TSetContext ctx;

  public ArrowBasedSinkFunc(String filepath, int parallelism) {
    LOG.info("%%%%%%%%%%%%% arrow based sink function getting called%%%%%%%%");
    this.parallel = parallelism;
    this.arrowfileName = filepath;
  }

  @Override
  public void prepare(TSetContext context) {
    this.ctx = context;
    try {
      File outFile = new File(arrowfileName);
      outFile.getParentFile().mkdirs();
      writer = new BufferedWriter(new FileWriter(outFile, false));
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to create arrow file", ioe);
    }
    LOG.info("%%%%%%%%%%%%% Prepare function getting called%%%%%%%%");
  }

  @Override
  public void close() {
    //outputWriter.close();
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to close the writer!");
    }
  }

  @Override
  public DataPartition<?> get() {
    return null;
  }

  @Override
  public boolean add(Integer value) {
    LOG.info("I am getting called:" + value);
    //while (value.hasNext()) {
    try {
      writer.write(String.valueOf(value/*.next()*/));
      writer.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
    //}
    return true;
  }
}
