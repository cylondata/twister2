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
package edu.iu.dsc.tws.task.tsystem;

import java.util.ArrayList;
import java.util.List;

/**
 * This class will be used during the construction of task graph objects...!
 */
public class TaskOutputFiles {

  private List<Object> outputFiles = new ArrayList<Object>();

  public List<Object> getOutputFiles() {
    return outputFiles;
  }

  public void setOutputFiles(List<Object> outputFiles) {
    this.outputFiles = outputFiles;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskOutputFiles)) {
      return false;
    }

    TaskOutputFiles that = (TaskOutputFiles) o;

    return outputFiles != null ? outputFiles.equals(that.outputFiles) : that.outputFiles == null;
  }

  @Override
  public int hashCode() {
    return outputFiles != null ? outputFiles.hashCode() : 0;
  }
}

