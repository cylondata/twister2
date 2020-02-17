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
package edu.iu.dsc.tws.python.numpy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NumpyHolder implements Serializable {

  private Object numpyFlatArray;
  private Object type;
  private Object shape;

  public NumpyHolder() {

  }

  public NumpyHolder(Object numpyFlatArray, Object shape, Object type) {
    this.numpyFlatArray = numpyFlatArray;
    this.type = type;
    this.shape = new ArrayList<>((List) shape);
  }

  public void setNumpyFlatArray(Object numpyFlatArray) {
    this.numpyFlatArray = numpyFlatArray;
  }

  public void setType(Object type) {
    this.type = type;
  }

  public void setShape(Object shape) {
    this.shape = shape;
  }

  public Object getNumpy() {
    return numpyFlatArray;
  }

  public Object getType() {
    return type;
  }

  public Object getShape() {
    return shape;
  }
}
