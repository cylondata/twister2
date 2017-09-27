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
package edu.iu.dsc.tws.common.config;

/**
 * Class that holds common config constants and there default values
 */
public final class ConfigConstants {

  /**
   * The default filesystem to be used, if no other scheme is specified in the
   * user-provided URI (= local filesystem)
   * */
  public static final String DEFAULT_FILESYSTEM_SCHEME = "file:///";

  /**
   * Key to specify the default filesystem to be used by a job. In the case of
   * <code>file:///</code>, which is the default (see {@link ConfigConstants#DEFAULT_FILESYSTEM_SCHEME}),
   * the local filesystem is going to be used to resolve URIs without an explicit scheme.
   * */
  public static final String FILESYSTEM_SCHEME = "fs.default-scheme";

  private ConfigConstants() {

  }
}
