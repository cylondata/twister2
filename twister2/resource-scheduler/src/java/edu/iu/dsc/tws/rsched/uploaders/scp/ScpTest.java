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
package edu.iu.dsc.tws.rsched.uploaders.scp;

import java.net.URI;

import edu.iu.dsc.tws.common.config.Config;

public final class ScpTest {

  private ScpTest() {
  }

  public static void main(String[] args) {


    Config config = Config.newBuilder().put(ScpContext.UPLOAD_DIRECTORY, "/vagrant").
        put(ScpContext.TWISTER2_UPLOADER_SCP_OPTIONS, "").
        put(ScpContext.TWISTER2_UPLOADER_SCP_CONNECTION, "root@149.165.150.81").
        put(ScpContext.TWISTER2_UPLOADER_SSH_OPTIONS, "-i /Users/user1/.ssh/id_rsa").
        put(ScpContext.TWISTER2_UPLOADER_SSH_CONNECTION, "root@149.165.150.81").build();

    ScpUploader uploader = new ScpUploader();
    uploader.initialize(config);

    //upload
    URI destURI = uploader.uploadPackage("/Users/user1/Desktop/tobecopied");
    System.out.println("File path in remote machine is " + destURI);


    //delete
    //uploader.undo();
    //System.out.println("File deleted in remote machine");

  }

}

