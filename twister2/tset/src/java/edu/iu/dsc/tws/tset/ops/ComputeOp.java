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

package edu.iu.dsc.tws.tset.ops;

import java.util.Map;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Performs the compute function on the value received for the imessage and write it to edges
 */
public class ComputeOp<O, I> extends BaseComputeOp<I> {

  private ComputeFunc<O, I> computeFunction;

  public ComputeOp() {
  }

  public ComputeOp(ComputeFunc<O, I> computeFunction, BaseTSet origin,
                   Map<String, String> receivables) {
    super(origin, receivables);
    this.computeFunction = computeFunction;
  }

  @Override
  public TFunction getFunction() {
    return computeFunction;
  }

  @Override
  public boolean execute(IMessage<I> content) {
    O output = computeFunction.compute(content.getContent());
    writeToEdges(output);
    writeEndToEdges();
    computeFunction.close();
    return true;
  }
}
