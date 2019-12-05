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

import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;

public abstract class CSVInputPartitioner<OT> implements
    InputPartitioner<OT, FileInputSplit<OT>> {

  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_LINE_DELIMITER = "\n";

  public static final String DEFAULT_FIELD_DELIMITER = ",";

  protected transient Object[] parsedValues;

  protected CSVInputPartitioner(Path filePath) {
  }
}
