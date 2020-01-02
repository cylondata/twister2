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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.assigner.OrderedInputSplitAssigner;
//import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
//import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class LocalCSVInputPartitioner extends CSVInputPartitioner {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(LocalCSVInputPartitioner.class.getName());

  private Config config;
  private int numberOfTasks;
  private int recordLength;

  private OrderedInputSplitAssigner<String> assigner;

  public LocalCSVInputPartitioner(Path filePath, int recordLen) {
    super(filePath);
    this.recordLength = recordLen;
  }

  public LocalCSVInputPartitioner(Path filePath, int nTasks, int recordLen, Config cfg) {
    super(filePath, recordLen, nTasks, cfg);
    this.numberOfTasks = nTasks;
    this.recordLength = recordLen;
    this.config = cfg;
  }

  public LocalCSVInputPartitioner(Path filePath, int numTasks, Config config) {
    super(filePath, config);
    this.numberOfTasks = numTasks;
  }

  protected CSVInputSplit createSplit(int num, Path file, long start,
                                      long length, String[] hosts) {
    return new CSVInputSplit(num, file, start, length, hosts);
  }

  public InputSplitAssigner getInputSplitAssigner(FileInputSplit[] inputSplits) {
    if (assigner == null) {
      assigner = new OrderedInputSplitAssigner<>(inputSplits, numberOfTasks);
    }
    return assigner;
  }
}
