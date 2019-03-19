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
package edu.iu.dsc.tws.api.tset.sources;

import java.io.IOException;

import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.data.api.formatters.FileInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class FileSource<T> implements Source<T> {
  /**
   * The input partitioner
   */
  private FileInputPartitioner<T> inputPartitioner;

  /**
   * Splits created
   */
  private FileInputSplit<T>[] splits;

  /**
   * The current split we are using
   */
  private InputSplit<T> currentSplit;

  /**
   * The context
   */
  private TSetContext tSetContext;

  public FileSource(FileInputPartitioner<T> input) {
    this.inputPartitioner = input;
  }

  @Override
  public boolean hasNext() {
    try {
      if (currentSplit == null) {
        return false;
      }
      if (currentSplit.reachedEnd()) {
        currentSplit = getNextSplit(tSetContext.getIndex());
        if (currentSplit == null) {
          return false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to use the input split", e);
    }
    return true;
  }

  @Override
  public T next() {
    if (currentSplit == null) {
      throw new IllegalStateException("Need to check hasNext before calling next");
    }

    try {
      return currentSplit.nextRecord(null);
    } catch (IOException e) {
      throw new RuntimeException("Failed to ");
    }
  }

  @Override
  public void prepare(TSetContext context) {
    this.tSetContext = context;
    this.inputPartitioner.configure(context.getConfig());
    try {
      this.splits = inputPartitioner.createInputSplits(context.getParallelism());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create the input splits");
    }
    this.currentSplit = getNextSplit(context.getIndex());
  }

  @Override
  public void prepare() {

  }

  private InputSplit<T> getNextSplit(int id) {
    InputSplitAssigner<T> assigner = inputPartitioner.getInputSplitAssigner(splits);
    InputSplit<T> split = assigner.getNextInputSplit("localhost", id);
    if (split != null) {
      try {
        split.open();
      } catch (IOException e) {
        throw new RuntimeException("Failed to open split", e);
      }
      return split;
    } else {
      return null;
    }
  }
}
