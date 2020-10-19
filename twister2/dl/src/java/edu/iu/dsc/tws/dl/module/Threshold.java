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
import edu.iu.dsc.tws.dl.utils.Util;
import edu.iu.dsc.tws.dl.utils.pair.TensorArrayPair;

public class Threshold extends TensorModule {

  private double threshold = 1e-6;
  private double value = 0.0;
  private boolean inPlace = false;

  public Threshold() {
    validateParameters();
  }

  public Threshold(double threshold, double value, boolean inPlace) {
    this.threshold = threshold;
    this.value = value;
    this.inPlace = inPlace;
    validateParameters();
  }

  @Override
  public DenseTensor updateOutput(DenseTensor input) {
//    require(input.isContiguous())
//    validateParameters()
//
//    val taskSize = input.nElement() / Engine.model.getPoolSize
//    var extraTaskSize = input.nElement() % Engine.model.getPoolSize
//    var allocated = 0
//    val tasks = new ArrayBuffer[(Int, Int)]()
//    while (allocated < input.nElement()) {
//      val end = math.min(input.nElement(), if (extraTaskSize > 0) {
//        extraTaskSize -= 1
//        allocated + taskSize + 1
//      } else {
//        allocated + taskSize
//      })
//      tasks += ((allocated, end))
//      allocated = end
//    }
//
//    val taskArray = tasks.toArray
//    val results = new Array[Future[Unit]](taskArray.length)
//
//    if (inPlace) {
//      output = input
//      val inputDouble = input.asInstanceOf[Tensor[Double]]
//      val inputData = inputDouble.storage().array()
//      val inputOffset = inputDouble.storageOffset() - 1
//
//      var t = 0
//      while (t < taskArray.length) {
//        val _t = t
//        results(_t) = Engine.model.invoke(() => {
//            var i = taskArray(_t)._1
//        while (i < taskArray(_t)._2) {
//          inputData(inputOffset + i) =
//          if (inputData(inputOffset + i) <= threshold) {
//            value
//          } else {
//            inputData(inputOffset + i)
//          }
//          i += 1
//        }
//            })
//        t += 1
//      }
//      input
//    }
//    else {
//      output.asInstanceOf[Tensor[Double]].resizeAs(input.asInstanceOf[Tensor[Double]])
//
//      val inputDouble = input.asInstanceOf[Tensor[Double]]
//      val inputData = inputDouble.storage().array()
//      val inputOffset = inputDouble.storageOffset() - 1
//      val outputDouble = output.asInstanceOf[Tensor[Double]]
//      val outputData = outputDouble.storage().array()
//      val outputOffset = outputDouble.storageOffset() - 1
//
//      var t = 0
//      while (t < taskArray.length) {
//        val _t = t
//        results(_t) = Engine.model.invoke(() => {
//            var i = taskArray(_t)._1
//        while (i < taskArray(_t)._2) {
//          outputData(outputOffset + i) =
//          if (inputData(inputOffset + i) > threshold) {
//            inputData(inputOffset + i)
//          } else {
//            value
//          }
//          i += 1
//        }
//            })
//        t += 1
//      }
//    }
//    Engine.model.sync(results)
//    output

  }

  @Override
  public DenseTensor updateGradInput(DenseTensor input, DenseTensor gradOutput) {
    return null;
  }

  @Override
  public TensorArrayPair parameters() {
    return null;
  }

  @Override
  public void reset() {

  }

  public void validateParameters(){
    if (inPlace) {
      Util.require(value <= threshold, "in-place processing requires value (" +
          value + "') not exceed threshold (" + threshold + ")");
    }
  }



}
