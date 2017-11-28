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

public final class Context {

  private Context() {
  }

  public static Double instanceRam(Config config) {
    System.out.println(config.getDoubleValue(Key.INSTANCE_RAM));
    //return config.getDoubleValue(Key.INSTANCE_RAM);
    return Double.valueOf(20.0);
  }

  public static Double instanceDisk(Config config) {
    //return config.getDoubleValue (Key.INSTANCE_DISK);
    return Double.valueOf(200.0);
  }

  public static Double instanceCPU(Config config) {
    //return config.getDoubleValue (Key.INSTANCE_CPU);
    return Double.valueOf(5.0);
  }
}
