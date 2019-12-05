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
package edu.iu.dsc.tws.data.api.formatters;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class LocalCSVInputPartitioner<T> extends CSVInputPartitioner<T> {

  public LocalCSVInputPartitioner(Path filePath, int parallelism, Config cfg) {
    super(filePath);
  }

  @Override
  public void configure(Config parameters) {
  }

  @Override
  public FileInputSplit<T>[] createInputSplits(int minNumSplits) throws Exception {
    return new FileInputSplit[0];
  }

  @Override
  public InputSplitAssigner<T> getInputSplitAssigner(FileInputSplit<T>[] inputSplits) {
    return null;
  }
}
