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
package edu.iu.dsc.tws.tsched.taskprogrammingmodel.task;

import edu.iu.dsc.tws.tsched.taskprogrammingmodel.Constants;
import edu.iu.dsc.tws.tsched.taskprogrammingmodel.Constraints;

public @interface Method {

    String declaringClass();

    String name() default Constants.UNASSIGNED;

    String isModifier() default Constants.IS_MODIFIER;

    String priority() default Constants.IS_NOT_PRIORITY_TASK;

    Constraints constraints() default @Constraints();

}

