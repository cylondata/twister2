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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.assigner.LocatableInputSplitAssigner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.data.fs.Path;

public class SharedTextInputPartitioner extends FileInputPartitioner<String> {
  private static final long serialVersionUID = 1L;

  private LocatableInputSplitAssigner<String> assigner;

  public SharedTextInputPartitioner(Path filePath) {
    super(filePath);
  }

  public SharedTextInputPartitioner(Path filePath, int numTasks, Config config) {
    super(filePath, config);
  }

  @Override
  protected FileInputSplit createSplit(int num, Path file, long start,
                                       long length, String[] hosts) {
    return new TextInputSplit(num, file, start, length, hosts);
  }

  @Override
  public LocatableInputSplitAssigner<String> getInputSplitAssigner(
      FileInputSplit<String>[] inputSplits) {
    if (assigner == null) {
      assigner = new LocatableInputSplitAssigner<String>(inputSplits);
    }
    return assigner;
  }
}
