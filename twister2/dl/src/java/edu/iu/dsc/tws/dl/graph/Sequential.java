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

import java.util.ArrayList;
import java.util.List;

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
    int i = modules.size() - 1;
    AbstractModule currentModule = modules.get(i);
    DenseTensor currentGradOutput = gradOutput;
    while (i > 0) {
      AbstractModule previousModule = modules.get(i - 1);
      currentModule.accGradParameters((DenseTensor) previousModule.output, currentGradOutput);
      currentGradOutput = (DenseTensor) currentModule.gradInput;
      currentModule = previousModule;
      i -= 1;
    }

    currentModule.accGradParameters(input, currentGradOutput);
  }

  @Override
  public DenseTensor backward(DenseTensor input, DenseTensor nextError) {
    long before = System.nanoTime();
    int i = modules.size() - 1;
    DenseTensor error = nextError;
    while (i > 0) {
      Activity inputLocal = modules.get(i - 1).output;
      error = modules.get(i).backward(input, error);
      i -= 1;
    }
    error = modules.get(0).backward(input, error);

    this.gradInput = error;
    backwardTime += System.nanoTime() - before;
    return (DenseTensor) gradInput;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    if (!(obj instanceof Sequential)) {
      return false;
    }
    Sequential other = (Sequential)obj;
    if (this == other) {
      return true;
    }

    if (this.modules.size() != other.modules.size()) {
      return false;
    }

    int moduleLength = modules.size();
    int i = 0;
    while (i < moduleLength) {
      if (modules.get(i) != other.modules.get(i)) {
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
    String tab = "  ";
    StringBuilder message = new StringBuilder()
        .append(getPrintName() + line + tab)
        .append("[input -> ");

    List<String> temp = new ArrayList<>();
    for (int i = 0; i < modules.size(); i++) {
       if(modules.get(i) instanceof AbstractModule) {
         temp.add("(" + (i+1) + ")");
       }
    }

    message.append(String.join(" -> ", temp))
        .append("-> output]" + line + tab);

    temp = new ArrayList<>();
    for (int i = 0; i < modules.size(); i++) {
      if(modules.get(i) instanceof AbstractModule) {
        temp.add("(" + (i+1) + "):" + modules.get(i).setLine(line + tab));
      }
    }

    message.append(String.join(line + tab, temp))
        .append(line + "}");

    return message.toString();
  }

  @Override
  public Node<AbstractModule>[] getEndNodes(Node<AbstractModule>[] startNodes) {
    Node<AbstractModule>[] startnodes = startNodes;
    Node<AbstractModule>[] curNodes = null;
    for (int i = 0; i < modules.size(); i++) {
      curNodes = modules.get(i).getEndNodes(startnodes);
      startnodes = curNodes;
    }
    return curNodes;
  }
}
