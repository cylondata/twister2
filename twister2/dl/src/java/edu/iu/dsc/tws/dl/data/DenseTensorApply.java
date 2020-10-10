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
package edu.iu.dsc.tws.dl.data;

import edu.iu.dsc.tws.dl.data.function.TensorFunc2;
import edu.iu.dsc.tws.dl.data.function.TensorFunc4;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.utils.Util;

@SuppressWarnings("LocalVariableName")
public final class DenseTensorApply {

  private DenseTensorApply() {
  }

  public static void apply1(DenseTensor tensor, TensorFunc2 func) {
    if (tensor.isEmpty()) {
      return;
    }

    double[] data = tensor.storage().toDoubleArray();
    int index = tensor.storageOffset() - 1;

    if (tensor.isScalar()) {
      func.apply(data, index);
      return;
    }

    int stride = getStride(tensor);
    int[] meta = getLargestContiguousSize(tensor);
    int[] counter = getCounter(meta[1]);
    // store if finished and new offset
    int[] counterMeta = new int[2];
    counterMeta[0] = 0; // hasFinished
    counterMeta[1] = tensor.storageOffset() - 1; //offset
    int offset = counterMeta[1];
    int hasFinished = counterMeta[0];
    int i = 0;
    while (hasFinished == 0) {
      while (i < meta[0]) {
        index = offset + i * stride;
        func.apply(data, index);
        i += 1;
      }
      updateCounter(tensor, counter, offset, meta[1], counterMeta);
      hasFinished = counterMeta[0];
      offset = counterMeta[1];
      i = 0;
    }
  }

  /**
   * Iterate through tensor1, tensor2, and apply func to the elements
   *
   * @param tensor1 the tensor
   * @param tensor2 the tensor
   * @param func    (tensor1Data, tensor1Offset, tensor2Data, tensor2Offset)
   */
  public static void apply2(DenseTensor tensor1, Tensor tensor2, TensorFunc4 func) {
    Util.require(tensor1.nElement() == tensor2.nElement(),
        "inconsistent tensor size: ${tensor1.nElement()} == ${tensor2.nElement()}");

    if (tensor1.isEmpty()) {
      return;
    }

    // shortcut for scalar
    if (tensor1.isScalar() && tensor2.isScalar()) {
      double[] tensor1Data = tensor1.storage().toDoubleArray();
      double[] tensor2Data = tensor2.storage().toDoubleArray();
      int tensor1Index = tensor1.storageOffset() - 1;
      int tensor2Index = tensor2.storageOffset() - 1;
      func.apply(tensor1Data, tensor1Index, tensor2Data, tensor2Index);
      return;
    }

    double[] tensor1Data = tensor1.storage().toDoubleArray();
    int tensor1Offset = tensor1.storageOffset() - 1;
    double[] tensor2Data = tensor2.storage().toDoubleArray();
    int tensor2Offset = tensor2.storageOffset() - 1;

    boolean adjacent = false;
    if (tensor1.nDimension() == 1 && tensor2.nDimension() == 1 && tensor1.stride(1) == 1
        && tensor2.stride(1) == 1) {
      adjacent = true;
    }
    if (tensor1.nDimension() == 2 && tensor2.nDimension() == 2) {
      if (tensor1.stride(2) == 1 && tensor2.stride(2) == 1 && tensor1.stride(1) == tensor1.size(2)
          && tensor2.stride(1) == tensor2.size(2)) {
        adjacent = true;
      }

      if (tensor1.stride(1) == 1 && tensor2.stride(1) == 1 && tensor1.stride(2) == tensor1.size(1)
          && tensor2.stride(2) == tensor2.size(1)) {
        adjacent = true;
      }
    }
    if (adjacent) {
      int i = 0;
      while (i < tensor1.nElement()) {
        func.apply(tensor1Data, tensor1Offset + i, tensor2Data, tensor2Offset + i);
        i += 1;
      }
      return;
    }

    int tensor1Stride = getStride(tensor1);
    int[] meta1 = getLargestContiguousSize(tensor1);
    int[] counter1 = getCounter(meta1[1]);
    int tensor2Stride = getStride(tensor2);
    int[] meta2 = getLargestContiguousSize(tensor2);
    int[] counter2 = getCounter(meta2[1]);
    int[] counterMeta1 = new int[2];
    int[] counterMeta2 = new int[2];

    int hasFinished = 0;
    int i1 = 0;
    int i2 = 0;
    while (hasFinished == 0) {
      while (i1 < meta1[0] && i2 < meta2[0]) {
        func.apply(tensor1Data, tensor1Offset + i1 * tensor1Stride, tensor2Data,
            tensor2Offset + i2 * tensor2Stride);
        i1 = i1 + 1;
        i2 = i2 + 1;
      }

      if (i1 == meta1[0]) {
        updateCounter(tensor1, counter1, tensor1Offset, meta1[1], counterMeta1);
        hasFinished = counterMeta1[0];
        tensor1Offset = counterMeta1[1];
        i1 = 0;
      }

      if (i2 == meta2[0]) {
        updateCounter(tensor2, counter2, tensor2Offset, meta2[1], counterMeta2);
        hasFinished = counterMeta2[0];
        tensor2Offset = counterMeta2[1];
        i2 = 0;
      }
    }
  }

  private static void updateCounter(Tensor tensor, int[] counter, int offset, int dim,
                                    int[] counterMeta) {
    if (dim == 0) {
      return;
    }

    int _offset = offset;
    int i = dim;
    while (i > 0) {
      counter[i - 1] += 1;
      _offset += tensor.stride(i);
      if (counter[i - 1] == tensor.size(i)) {
        if (i == 1) {
          counterMeta[0] = 1;
          counterMeta[1] = _offset;
          return;
        } else {
          _offset -= counter[i - 1] * tensor.stride(i);
          counter[i - 1] = 0;
        }
      } else {
        counterMeta[0] = 0;
        counterMeta[1] = _offset;
        return;
      }
      i -= 1;
    }

    counterMeta[0] = 0;
    counterMeta[1] = _offset;
    return;
  }

  private static int[] getCounter(int largestDim) {
    int[] counter = new int[largestDim];
    int d = 0;
    while (d < largestDim) {
      counter[d] = 0;
      d += 1;
    }
    return counter;
  }

  /**
   * return largestSize and largestDim.
   */
  private static int[] getLargestContiguousSize(Tensor tensor) {
    int[] meta = new int[2];
    meta[0] = 1; //largestSize
    meta[1] = tensor.nDimension(); //largestDim
    while (meta[1] > 0) {
      if (tensor.size(meta[1]) != 1) {
        if (tensor.stride(meta[1]) == meta[0]) {
          meta[0] = meta[0] * tensor.size(meta[1]);
        } else {
          return meta;
        }
      }
      meta[1] -= 1;
    }
    return meta;
  }

  private static int getStride(Tensor tensor) {
    int d = tensor.nDimension();
    while (d > 0) {
      if (tensor.size(d) != 1) {
        return tensor.stride(d);
      }
      d -= 1;
    }
    return 0;
  }
}
