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
package edu.iu.dsc.tws.data;


import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

/**
 * Test class for LocalFileSystem
 */
public class TestLocalFileSystem {

  public static void main(String[] args) {
    Config.Builder builder = new Config.Builder();
    builder.put("input.file.path", "/home/pulasthi/git/twister2/twister2/data/src/test"
        + "/resources/TextInputFormatTestFile.text");
    Config txtFileConf = builder.build();
    Path path = new Path("/home/pulasthi/git/twister2/twister2/data/src/test/resources"
        + "/TextInputFormatTestFile.text");
    InputPartitioner txtInput = new SharedTextInputPartitioner(path);
    txtInput.configure(txtFileConf);
    int minSplits = 8;

    try {
      InputSplit[] inputSplits = txtInput.createInputSplits(minSplits);
      InputSplitAssigner inputSplitAssigner = txtInput.getInputSplitAssigner(inputSplits);
      InputSplit cur = inputSplitAssigner.getNextInputSplit(null, 0);
      cur.open();
      String line = "";
      line = (String) cur.nextRecord(line);
      System.out.println(line);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
