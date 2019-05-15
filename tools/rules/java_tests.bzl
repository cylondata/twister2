#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

def java_tests(test_classes, runtime_deps=[], resources=[], data=[], size="medium"):
    for test_class in test_classes:
        native.java_test(
            name = test_class.split(".")[-1],
            runtime_deps = runtime_deps,
            size = size,
            test_class = test_class,
            resources = resources,
            data = data,
        )

def java_tests_debug(test_classes, runtime_deps=[], resources=[], data=[], size="medium"):
    for test_class in test_classes:
        print(test_class.split(".")[-1] + "_debug")
        native.java_test(
            name = test_class.split(".")[-1] + "_debug",
            jvm_flags=["-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"],
            runtime_deps = runtime_deps,
            size = size,
            test_class = test_class,
            resources = resources,
            data = data,
        )