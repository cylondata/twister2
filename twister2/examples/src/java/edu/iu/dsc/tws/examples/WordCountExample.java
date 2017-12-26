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
package edu.iu.dsc.tws.examples;

import java.io.FileWriter;
import java.io.IOException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.api.formatters.TextInputFormatter;
import edu.iu.dsc.tws.data.fs.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;

/**
 * WordCount example based on developed on Twister 2
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WordCountExample implements IContainer {

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    Config.Builder builder = new Config.Builder();
    builder.put("input.file.path", "/home/pulasthi/git/twister2/twister2"
        + "/data/src/test/resources/TextInputFormatTestFile.text");
    Config txtFileConf = builder.build();
    Path path = new Path("/home/pulasthi/git/twister2/twister2/data/src/test/"
        + "resources/TextInputFormatTestFile.text");
    @SuppressWarnings("unchecked") InputFormat txtInput = new TextInputFormatter(path);
    txtInput.configure(txtFileConf);
    int minSplits = 8;

    try {
      InputSplit[] inputSplits = txtInput.createInputSplits(minSplits);
      InputSplitAssigner inputSplitAssigner = txtInput.getInputSplitAssigner(inputSplits);
      MapperSource mapperSource = new MapperSource(txtInput, inputSplitAssigner.getNextInputSplit(
          null, id));
      Mapper wordCountMapper = new Mapper();
      wordCountMapper.setInputSource(mapperSource);

    } catch (Exception e) {
      e.printStackTrace();
    }


  }

  private class Mapper extends SourceTask<MapperSource> {

    @Override
    public Message execute() {
      try {
        InputFormat<String, FileInputSplit> txtInput = getInputSource().getInputFormat();
        FileInputSplit inputSplits = (FileInputSplit) getInputSource().getInputSplit();
        txtInput.open(inputSplits);
        String line = "";
        line = txtInput.nextRecord(line);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public Message execute(Message content) {
      return null;
    }
  }

  private class Reducer extends SinkTask<FileWriter> {

    @Override
    public Message execute() {
      return null;
    }

    @Override
    public Message execute(Message content) {
      return null;
    }
  }

  private class MapperSource {

    private InputFormat inputFormat;
    private InputSplit inputSplit;

    MapperSource(InputFormat inf, InputSplit ins) {
      this.inputFormat = inf;
      this.inputSplit = ins;
    }

    public InputFormat getInputFormat() {
      return inputFormat;
    }

    public void setInputFormat(InputFormat inputFormat) {
      this.inputFormat = inputFormat;
    }

    public InputSplit getInputSplit() {
      return inputSplit;
    }

    public void setInputSplit(InputSplit inputSplit) {
      this.inputSplit = inputSplit;
    }

  }
}
