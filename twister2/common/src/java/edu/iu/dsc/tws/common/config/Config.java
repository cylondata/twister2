//  Copyright 2017 Twitter. All rights reserved.
//
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


import java.util.logging.Logger;

/**
 * Config is an Immutable Map of &lt;String, Object&gt; The get/set API that uses Key objects
 * should be favored over Strings. Usage of the String API should be refactored out.
 *
 * A newly created Config object holds configs that might include wildcard tokens, like
 * ${HERON_HOME}/bin, ${HERON_LIB}/packing/*. Token substitution can be done by converting that
 * config to a local or cluster config by using the {@code Config.toLocalMode} or
 * {@code Config.toClusterMode} methods.
 *
 * Local mode is for a config to be used to run Heron locally, where HERON_HOME might be an install
 * dir on the local host (e.g. HERON_HOME=/usr/bin/heron). Cluster mode is to be used when building
 * configs for a remote process run on a service, where all directories might be relative to the
 * current dir by default (e.g. HERON_HOME=~/heron-core).
 */
public class Config {
  private static final Logger LOG = Logger.getLogger(Config.class.getName());

  public String getStringValue(Key key) {
    return null;
  }
}