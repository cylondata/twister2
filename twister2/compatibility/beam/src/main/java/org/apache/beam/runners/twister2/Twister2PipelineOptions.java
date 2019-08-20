/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.twister2;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

import edu.iu.dsc.tws.api.tset.TSetEnvironment;


/**
 * doc.
 */
public interface Twister2PipelineOptions extends PipelineOptions, StreamingOptions {
  @Description("set unique application name for Twister2 runner")
  void setApplicationName(String name);

  String getApplicationName();

  @Description("set parallelism for Gearpump processor")
  void setParallelism(int parallelism);

  @Default.Integer(1)
  int getParallelism();

  @Description("register Kryo serializers")
  void setSerializers(Map<String, String> serializers);

  @JsonIgnore
  Map<String, String> getSerializers();

  //  @Description("Twister2 batch execution context")
  //  void setBatchExecutionContext(TwisterBatchContext tbc);
  //
  //  TwisterBatchContext getBatchExecutionContext();
  //
  //  @Description("Twister2 stream execution context")
  //  void setStreamExecutionContext(TwisterStreamingContext tsc);
  //
  //  TwisterStreamingContext getStreamExecutionContext();

  @Description("Twister2 TSetEnvironment")
  void setTSetEnvironment(TSetEnvironment environment);

  @JsonIgnore
  TSetEnvironment getTSetEnvironment();
}
