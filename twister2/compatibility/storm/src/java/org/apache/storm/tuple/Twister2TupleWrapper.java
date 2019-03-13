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
package org.apache.storm.tuple;

import java.util.List;

/**
 * Twister2 engine currently has a problem when handling message contents which inherits
 * from {@link java.util.List}. This class will be temporary used to address this issue.
 * Once this is fixed. This class should be removed. User's code won't be affected
 * with these changes.
 */
public class Twister2TupleWrapper {

  private List stormValue;

  public Twister2TupleWrapper(List value) {
    this.stormValue = value;
  }

  public List getStormValue() {
    return stormValue;
  }

  public void setStormValue(List stormValue) {
    this.stormValue = stormValue;
  }
}
