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

import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;

public class CSVBasedSourceFunction<T> extends BaseSourceFunc<String[]> {

  private static final Logger LOG = Logger.getLogger(CSVBasedSourceFunction.class.getName());

  private TextBasedSourceFunction<String> textSource;
  private Pattern pattern;

  public CSVBasedSourceFunction(String dataInputdirectory, int datasize,
                                int parallelism, String type) {
    this.textSource = new TextBasedSourceFunction<>(dataInputdirectory, datasize,
        parallelism, type);
    pattern = Pattern.compile(CSVInputSplit.DEFAULT_FIELD_DELIMITER);
  }

  @Override
  public void prepare(TSetContext context) {
    textSource.prepare(context);
  }

  @Override
  public boolean hasNext() {
    return textSource.hasNext();
  }

  @Override
  public String[] next() {
    return pattern.split(textSource.next());
  }
}
