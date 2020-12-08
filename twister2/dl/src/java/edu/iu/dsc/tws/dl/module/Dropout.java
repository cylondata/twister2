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
package edu.iu.dsc.tws.dl.module;

import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.graph.TensorModule;
import edu.iu.dsc.tws.dl.utils.Shape;
import edu.iu.dsc.tws.dl.utils.pair.TensorArrayPair;

/**
 * Dropout masks(set to zero) parts of input using a bernoulli distribution.
 * Each input element has a probability initP of being dropped. If `scale` is
 * true(true by default), the outputs are scaled by a factor of `1/(1-initP)`
 * during training.
 * During eintuating, output is the same as input.
 *
 * It has been proven an effective approach for regularization and preventing
 * co-adaptation of feature detectors. For more details, plese see
 * [Improving neural networks by preventing co-adaptation of feature detectors]
 * (https://arxiv.org/abs/1207.0580)
 *
 * @param initP the probability p
 * @param inplace whether to make `input` and `output` share the same storage
 * @param scale whether to scale the output by a factor of `1 / (1 - p)`
 */
public class Dropout extends TensorModule {
  double initP = 0.5;
  boolean inplace = false;
  boolean scale = true;
  private double p = initP;
  DenseTensor noise = new DenseTensor();
  boolean isResampling = true;

  public Dropout() {
  }

  public Dropout(double initP) {
    this.initP = initP;
  }

  public Dropout(double initP, boolean inplace, boolean scale) {
    this.initP = initP;
    this.inplace = inplace;
    this.scale = scale;
  }

  @Override
  public DenseTensor updateOutput(DenseTensor input) {
    if (inplace) {
      this.output = input;
    } else {
      ((DenseTensor)this.output).resizeAs(input).copy(input);
    }

    if (train) {
      noise.resizeAs(input);
      if (input.isContiguous()) {
        if (isResampling) {
          double[] noiseData = noise.storage().toDoubleArray();
          int taskSize = noise.nElement() / Engine.model.getPoolSize;
          int extraTask = noise.nElement() % Engine.model.getPoolSize;
          int allocated = 0
          int offset = ((DenseTensor)this.output).storageOffset() - 1;
          int data = ((DenseTensor)this.output).storage.toDoubleArray();
          int i = 0
          while (allocated < noise.nElement()) {
            int start = allocated;
            allocated += taskSize;
            if (extraTask > 0) {
              allocated += 1;
              extraTask -= 1;
            }
            int end = allocated;
            results(i) = Engine.model.invoke(() => {
                int k = start;
            while (k < end) {
              noiseData(k) = if (RNG.bernoulli(1 - p)) {
                if (scale) {
                  data(offset + k) = ev.divide(data(offset + k), ev.fromType[Double](1 - p))
                  ev.fromType[Double](1.0 / (1 - p));
                } else {
                  ev.fromType[Int](1)
                }
              } else {
                data(offset + k) = ev.fromType[Int](0)
                ev.fromType[Int](0)
              }

              k += 1
            }
            })
            i += 1;

          }

          Engine.model.sync(results);
        } else {
          ((DenseTensor)this.output).cmul(noise)
        }
        this.output
      } else {
        if (isResampling) {
          noise.bernoulli(1 - p);

          if (scale) {
            noise.div(ev.fromType[Double](1 - p));
          }
        }

        ((DenseTensor)this.output).cmul(noise);
      }
    } else if (!scale) {
      ((DenseTensor)this.output).mul(ev.fromType[Double](1 - p));
    } else {
      output
    }

  }

  @Override
  public DenseTensor updateGradInput(DenseTensor input, DenseTensor gradOutput) {
    if (results == null) {
      results = new Array[Future[Unit]](Engine.model.getPoolSize);
    }
    if (train) {
      if (inplace) {
        this.gradInput = gradOutput
      } else {
        this.gradInput.resizeAs(gradOutput).copy(gradOutput);
      }

      if (gradInput.isContiguous()) {
        int noiseData = noise.storage().toDoubleArray();
        int taskSize = noise.nElement() / Engine.model.getPoolSize;
        int extraTask = noise.nElement() % Engine.model.getPoolSize;
        int gradInputData = gradInput.storage().toDoubleArray();
        int gradInputOffset = gradInput.storageOffset() - 1;
        int allocated = 0;
        int i = 0;
        while (allocated < noise.nElement()) {
          int start = allocated;
          allocated += taskSize;
          if (extraTask > 0) {
            allocated += 1;
            extraTask -= 1;
          }
          int end = allocated
          results(i) = Engine.model.invoke(() => {
              int k = start
          while (k < end) {
            gradInputData(gradInputOffset + k) =
                ev.times(gradInputData(gradInputOffset + k), noiseData(k))
            k += 1
          }
          })
          i += 1
        }

        Engine.model.sync(results);

        this.gradInput;
      } else {
        this.gradInput.cmul(noise);
      }
    } else {
      throw new IllegalArgumentException("backprop only defined while training")
    }

  }

  @Override
  public AbstractModule clearState() {
    return super.clearState();
  }

  @Override
  public Shape computeOutputShape(Shape inputShape) {
    return super.computeOutputShape(inputShape);
  }

  @Override
  public TensorArrayPair parameters() {
    return null;
  }

  @Override
  public void reset() {

  }
}
