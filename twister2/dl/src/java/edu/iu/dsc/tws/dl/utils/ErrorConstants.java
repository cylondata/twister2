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
package edu.iu.dsc.tws.dl.utils;

public final class ErrorConstants {
  private ErrorConstants() {
  }

  public static final String constrainInputAsVectorOrBatch =
          " The input to the layer needs to be a vector(or a mini-batch of vectors); \n" +
          " please use the Reshape module to convert multi-dimensional input into vectors " +
          "if appropriate";
}
