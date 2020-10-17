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
package edu.iu.dsc.tws.dl.graph;

import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import edu.iu.dsc.tws.dl.module.DynamicContainer;

public class Sequential extends DynamicContainer {

  @Override
  public DenseTensor updateOutput(DenseTensor input) {
    int i = 0;
    DenseTensor result = input;
    while (i < modules.size()) {
      result = modules.get(i).forward(result);
      i += 1;
    }

    this.output = result;
    return (DenseTensor) output;
  }

  @Override
  public DenseTensor updateGradInput(DenseTensor input, DenseTensor nextError) {
    int i = modules.size() - 1;
    DenseTensor error = nextError;
    while (i > 0) {
      DenseTensor inputTemp = (DenseTensor) modules.get(i - 1).output;
      error = modules.get(i).updateGradInput(inputTemp, error);
      i -= 1;
    }
    error = modules.get(0).updateGradInput(input, error);

    this.gradInput = error;
    return (DenseTensor) gradInput;
  }

  @Override
  public void accGradParameters(DenseTensor input, DenseTensor gradOutput) {
    super.accGradParameters(input, gradOutput);
  }

  @Override
  public DenseTensor backward(DenseTensor input, DenseTensor gradOutput) {
    return super.backward(input, gradOutput);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    if (!(obj instanceof Sequential)) {
      return false;
    }
    int other = obj.asInstanceOf[Sequential[T]]
    if (this.eq(other)) {
      return true;
    }

    if (this.modules.length != other.modules.length) {
      return false;
    }

    val moduleLength = modules.length;
    var i = 0;
    while (i < moduleLength) {
      if (modules(i) != other.modules(i)) {
        return false;
      }
      i += 1;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int seed = 37;
    int hash = super.hashCode();
    int moduleLength = modules.size();
    int i = 0;
    while (i < moduleLength) {
      hash = hash * seed + modules.get(i).hashCode();
      i += 1;
    }

    return hash;
  }

  @Override
  public String toString() {
    return "Sequential{}";
  }

  @Override
  protected Node<AbstractModule>[] getEndNodes(Node<AbstractModule>[] startNodes) {
    return super.getEndNodes(startNodes);
  }
}
