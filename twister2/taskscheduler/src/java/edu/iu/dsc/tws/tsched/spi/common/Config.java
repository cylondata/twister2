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
package edu.iu.dsc.tws.tsched.spi.common;

public final class Config {

  public static double containerMaxRAMValue = 10.00;
  public static double containerMaxCpuValue = 3.00;
  public static double containerMaxDiskValue = 10.00;
  public static int taskParallel = 2;

  //public static String schedulingMode = "RoundRobin";
  public static String schedulingMode = "FirstFit";

  private Config() {
  }

  public Double getDoubleValue(Key key) {
    Object value = get(key);
    Double dValue = 0.0;
    if (value instanceof Double) {
      dValue = (Double) value;
    }
    return dValue;
  }

  public Object get(Key key) {
    return get(key.value());
  }

  private Object get(String value) {
    return value;
  }
}
