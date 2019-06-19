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
package edu.iu.dsc.tws.examples.ml.svm.integration.test;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.assigner.OrderedInputSplitAssigner;
import edu.iu.dsc.tws.data.api.formatters.FileInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class LocalTextInputPartitioner1<T> extends FileInputPartitioner<T> {

  private static final Logger LOG = Logger.getLogger(LocalTextInputPartitioner1.class.getName());

  private static final long serialVersionUID = 4957141554227629138L;

  private int nTasks;

  private OrderedInputSplitAssigner<T> assigner;

  public LocalTextInputPartitioner1(Path filePath, int numTasks) {
    super(filePath);
    this.nTasks = numTasks;
  }

  public LocalTextInputPartitioner1(Path filePath, int numTasks, Config cfg) {
    super(filePath, cfg);
    this.nTasks = numTasks;
  }

  @Override
  protected TextInputSplit createSplit(int num, Path file, long start, long length,
                                       String[] hosts) {
    return new TextInputSplit(num, file, start, length, hosts);
  }

  @Override
  public InputSplitAssigner<T> getInputSplitAssigner(FileInputSplit<T>[] inputSplits) {
    if (assigner == null) {
      assigner = new OrderedInputSplitAssigner<T>(inputSplits, nTasks);
    }
    return assigner;
  }
}
