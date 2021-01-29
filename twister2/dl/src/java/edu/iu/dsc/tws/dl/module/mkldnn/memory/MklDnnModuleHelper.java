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
package edu.iu.dsc.tws.dl.module.mkldnn.memory;

import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.Tensor;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.mkldnn.MemoryData;
import edu.iu.dsc.tws.dl.module.mkldnn.MemoryOwner;
import edu.iu.dsc.tws.dl.module.mkldnn.memory.data.HeapData;
import edu.iu.dsc.tws.dl.module.mkldnn.memory.data.NativeData;
import edu.iu.dsc.tws.dl.utils.Util;

public class MklDnnModuleHelper extends MemoryOwner {

  transient protected MemoryOwner _this = this;

  protected Activity initActivity(MemoryData[] formats){
    if (formats.length == 1) {
      initTensor(formats[0]);
    } else {
      T.array(formats.map(initTensor(_)));
    }
  }

  protected Tensor initTensor(MemoryData format){
    int[] paddingShape = format.getPaddingShape();
    long realSize = format.getRealSize();

    if(format instanceof NativeData){
      switch (format.dataType()) {
        case DataType.S8:
          return new DnnTensor(paddingShape, realSize); //Byte
        case DataType.U8:
          return new DnnTensor(paddingShape, realSize); //Byte
        case DataType.S32:
          return new DnnTensor(paddingShape, realSize); //Int
        case DataType.F32:
          return new DnnTensor(paddingShape, realSize);
      }
    }else if(format instanceof MemoryData){
      return new DenseTensor(paddingShape);
    } else {
      throw new UnsupportedOperationException("memory format is not supported");
    }
    return null;
  }

  protected MemoryData[] singleNativeData(MemoryData[] formats){
    Util.require(formats.length == 1, "Only accept one tensor as input");
    nativeData(formats);
  }

  protected MemoryData[] nativeData(MemoryData[] formats) {
    MemoryData[] nativeDataArray = new MemoryData[formats.length];
    for (int i = 0; i < formats.length; i++) {
      MemoryData format = formats[i];
      if(!(format instanceof HeapData || format instanceof NativeData)){
        throw new UnsupportedOperationException("Not support memory format");
      }
      if(format instanceof HeapData) {
        format = ((HeapData)formats[i]).toNative();
      }
      nativeDataArray[i] = format;
    }
    return nativeDataArray;
  }
}

