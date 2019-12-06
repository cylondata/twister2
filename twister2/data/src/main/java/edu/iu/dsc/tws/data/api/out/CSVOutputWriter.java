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
package edu.iu.dsc.tws.data.api.out;

import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;

public class CSVOutputWriter extends FileOutputWriter<String> {

  public CSVOutputWriter(FileSystem.WriteMode writeMode, Path outPath) {
    super(writeMode, outPath);
  }

  @Override
  protected void createOutput(int partition, FSDataOutputStream out) {
  }

  @Override
  protected void writeRecord(int partition, String data) {
  }
}
