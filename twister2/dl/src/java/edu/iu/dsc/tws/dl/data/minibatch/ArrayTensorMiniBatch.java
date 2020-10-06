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
package edu.iu.dsc.tws.dl.data.minibatch;

import java.util.List;

import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.MiniBatch;
import edu.iu.dsc.tws.dl.data.Sample;
import edu.iu.dsc.tws.dl.data.Table;
import edu.iu.dsc.tws.dl.data.Tensor;

/**
 * Default type of MiniBatch.
 * This MiniBatch support both single/multi inputs and single/multi targets.
 * `inputData` store the input tensors, if `inputData.length == 1`, `getInput()` will return
 * a tensor; If `inputData.length > 1`, `getInput()` will return a table.
 * `targetData` store the target tensors, if `targetData.length == 1`, `getTarget()` will return
 * a tensor; If `targetData.length > 1`, `getTarget()` will return a table.
 *
 * @tparam T Numeric type
 */
public class ArrayTensorMiniBatch<T> implements MiniBatch<T> {
  //TODO: add padding support
//  featurePaddingParam feature padding strategy, see
//                             [[com.intel.analytics.bigdl.dataset.PaddingParam]] for details.
//  labelPaddingParam   label padding strategy, see
//                             [[com.intel.analytics.bigdl.dataset.PaddingParam]] for details.
  /**
   * a set of input tensor
   */
  private Tensor<T>[] inputData;

  /**
   * a set of target tensor
   */
  private Tensor<T>[] targetData;

  private Activity input;

  private Activity target;

  private int batchSize = 0;

  private boolean unlabaled = false;

  public ArrayTensorMiniBatch(int nInputs, int nTargets) {
    this.inputData = new Tensor[nInputs];
    this.targetData = new Tensor[nTargets];
    initInput();
  }

  public ArrayTensorMiniBatch(Tensor<T>[] input, Tensor<T>[] target) {
    this.inputData = input;
    this.targetData = target;
    initInput();
  }

  @Override
  public int size() {
    if (inputData[0].nElement() == 0) {
      return 0;
    } else {
      return inputData[0].size(1);
    }
  }

  @Override
  public MiniBatch<T> slice(int offset, int length) {
    Tensor<T>[] inputs = new Tensor[inputData.length];
    Tensor<T>[] targets = new Tensor[targetData.length];
    int b = 0;
    while (b < inputData.length) {
      inputs[b] = inputData[b].narrow(1, offset, length);
      b += 1;
    }
    b = 0;
    while (b < targetData.length) {
      targets[b] = targetData[b].narrow(1, offset, length);
      b += 1;
    }
    return new ArrayTensorMiniBatch<T>(inputs, targets);
  }

  @Override
  public Activity getInput() {
    return input;
  }

  @Override
  public Activity getTarget() {
    return target;
  }

  @Override
  public MiniBatch<T> set(List<Sample<T>> samples) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Init the input and target data structures
   */
  private void initInput() {
    if (inputData.length == 1) {
      this.input = inputData[0];
    } else {
      this.input = new Table(inputData);
    }

    if (targetData.length == 0) {
      this.target = null;
    } else if (targetData.length == 1) {
      this.target = targetData[0];
    } else {
      this.target = new Table(targetData);
    }
  }
}
