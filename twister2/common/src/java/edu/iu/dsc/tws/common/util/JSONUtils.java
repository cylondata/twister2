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
package edu.iu.dsc.tws.common.util;

import com.google.gson.Gson;

public final class JSONUtils {

  private JSONUtils() {
  }

  public static String toJSONString(Object obj) {
    Gson converter = new Gson();
    return converter.toJson(obj);
  }

  public static <T> T fromJSONString(String jsonString, Class<T> tClass) {
    Gson converter = new Gson();
    return converter.fromJson(jsonString, tClass);
  }
}
