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
package edu.iu.dsc.tws.rsched.uploaders.localfs;

import java.io.IOException;
import java.net.URI;

//import com.sun.jndi.toolkit.url.Uri;

import edu.iu.dsc.tws.common.config.Config;

public final class LFSTest {

  private LFSTest() {
  }

  public static void main(String[] args) throws IOException {

    Config config = Config.newBuilder().put(FsContext.UPLOAD_DIRECTORY,
                "/home/user/Desktop/test1/").build();
    LocalFileSystemUploader lsf = new LocalFileSystemUploader();
    lsf.initialize(config);
    URI returned = lsf.uploadPackage("/home/user/Desktop/tobecopied/");
    System.out.printf(returned.toString());
  }
}
