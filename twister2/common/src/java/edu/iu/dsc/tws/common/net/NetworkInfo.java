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
package edu.iu.dsc.tws.common.net;

import java.util.HashMap;
import java.util.Map;

public class NetworkInfo {
  private final int procId;

  private Map<String, Object> properties;

  public NetworkInfo(int procId) {
    this.procId = procId;
    this.properties = new HashMap<>();
  }

  public void addProperty(String prop, Object val) {
    properties.put(prop, val);
  }

  public void addAllProperties(Map<String, Object> props) {
    properties.putAll(props);
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Object getProperty(String prop) {
    return properties.get(prop);
  }

  public int getProcId() {
    return procId;
  }
}
