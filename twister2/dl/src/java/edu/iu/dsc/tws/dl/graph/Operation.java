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

import edu.iu.dsc.tws.dl.data.EmptyGradInput;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import sun.invoke.empty.Empty;

public abstract class Operation extends AbstractModule {

  public Operation() {
    this.gradInput = new EmptyGradInput(this.getName());
  }

  @Override
  public DenseTensor updateGradInput(DenseTensor input, DenseTensor gradOutput) {
    throw new UnsupportedOperationException("Operation does not support updateGradInput() method");
  }

  @Override
  public DenseTensor backward(DenseTensor input, DenseTensor gradOutput) {
    throw new UnsupportedOperationException("Operation does not support updateGradInput() method");
  }
}
