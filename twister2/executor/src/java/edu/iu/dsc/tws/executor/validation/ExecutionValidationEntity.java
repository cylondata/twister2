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
package edu.iu.dsc.tws.executor.validation;

import java.util.List;
import java.util.Objects;

public class ExecutionValidationEntity {

  private int sourceParallelism;

  private int sinkParallelism;

  private List<Integer> intermediateTaskParallelism;

  public ExecutionValidationEntity(int sourceParallelism, int sinkParallelism) {
    this.sourceParallelism = sourceParallelism;
    this.sinkParallelism = sinkParallelism;
  }

  public ExecutionValidationEntity(int sourceParallelism, int sinkParallelism,
                                   List<Integer> intermediateTaskParallelism) {
    this.sourceParallelism = sourceParallelism;
    this.sinkParallelism = sinkParallelism;
    this.intermediateTaskParallelism = intermediateTaskParallelism;
  }

  public int getSourceParallelism() {
    return sourceParallelism;
  }

  public void setSourceParallelism(int sourceParallelism) {
    this.sourceParallelism = sourceParallelism;
  }

  public int getSinkParallelism() {
    return sinkParallelism;
  }

  public void setSinkParallelism(int sinkParallelism) {
    this.sinkParallelism = sinkParallelism;
  }

  public List<Integer> getIntermediateTaskParallelism() {
    return intermediateTaskParallelism;
  }

  public void setIntermediateTaskParallelism(List<Integer> intermediateTaskParallelism) {
    this.intermediateTaskParallelism = intermediateTaskParallelism;
  }

  @Override
  public int hashCode() {

    return Objects.hash(getSourceParallelism(), getSinkParallelism(),
        getIntermediateTaskParallelism());
  }

  @Override
  public String toString() {
    return "ExecutionValidationEntity{"
        + "sourceParallelism=" + sourceParallelism
        + ", sinkParallelism=" + sinkParallelism
        + ", intermediateTaskParallelism=" + intermediateTaskParallelism + '}';
  }
}
