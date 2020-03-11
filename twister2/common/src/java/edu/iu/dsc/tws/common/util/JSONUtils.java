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

import java.io.IOException;

import com.google.gson.Gson;

public final class JSONUtils {

  private JSONUtils() {
  }

  public static <T> String toJSONString(Object obj, Class<T> tClass) {

    Gson converter = new Gson();
    return converter.toJson(obj, tClass);
  }

  public static <T> T fromJSONString(String jsonString, Class<T> tClass) {
    Gson converter = new Gson();
    return converter.fromJson(jsonString, tClass);
  }

  private static RuntimeTypeAdapterFactory<Exception> registerExceptionRuntimeTypes() {
    RuntimeTypeAdapterFactory<Exception> vehicleAdapterFactory
        = RuntimeTypeAdapterFactory.of(Exception.class, "type")

        .registerSubtype(RuntimeException.class, "runex")
        .registerSubtype(Exception.class, "exex")
        .registerSubtype(IllegalStateException.class, "isex")
        .registerSubtype(IOException.class, "ioex")
        .registerSubtype(NullPointerException.class, "nullpex");
    return vehicleAdapterFactory;
  }
}
