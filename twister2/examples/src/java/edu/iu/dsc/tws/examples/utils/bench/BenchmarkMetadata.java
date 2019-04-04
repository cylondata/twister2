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
package edu.iu.dsc.tws.examples.utils.bench;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkMetadata {

  public static final String ARG_BENCHMARK_METADATA = "bmeta";
  public static final String ARG_RUN_BENCHMARK = "runb";

  private String id;
  private String resultsFile;
  private List<BenchmarkArg> args = new ArrayList<>();

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getResultsFile() {
    return resultsFile;
  }

  public void setResultsFile(String resultsFile) {
    this.resultsFile = resultsFile;
  }

  public List<BenchmarkArg> getArgs() {
    return args;
  }

  public void setArgs(List<BenchmarkArg> args) {
    this.args = args;
  }
}
