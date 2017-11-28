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

@SuppressWarnings({"checkstyle:MethodParamPad", "checkstyle:LineLength"})

public enum Key {

  INSTANCE_RAM("twister2.resources.instance.ram", 4.0),
  INSTANCE_DISK("twister2.resources.instance.disk", 20.0),
  INSTANCE_CPU("twister2.resources.instance.cpu", 1.0);

  private String value;

  Key(Comparable<String> stringComparable, double v) {
  }

  public String value() {
    return value;
  }
}
