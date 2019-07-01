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
package edu.iu.dsc.tws.local.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.SecureClassLoader;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

/**
 * This class loader will be used to virtually create isolated contexts for Worker instances.
 * This will,
 *
 * <li>Exclude all classes in java. or sun. packages</li>
 * <li>Exclude {@link Twister2Job}, {@link Config} and {@link Config.Builder},
 * since they should be passed from parent loader to this loader</li>
 * <li>Exclude edu.iu.dsc.tws.proto package, since it has lot of inner classes and not </li>
 */
public class LocalClassLoader extends SecureClassLoader {

  private static final Logger LOG = Logger.getLogger(LocalClassLoader.class.getName());

  public Set<String> twsClassesToExclude = new HashSet<>();
  public Set<String> twsPackagesToExclude = new HashSet<>();
  public Set<String> classesToLoad = new HashSet<>();

  public LocalClassLoader(ClassLoader parent) {
    super(parent);
    // delegating following classes to parent class loader
    twsClassesToExclude.add(Twister2Job.class.getName());
    twsClassesToExclude.add(Config.class.getName());
    twsClassesToExclude.add(Config.Builder.class.getName());

    // delegating following packages to parent class loader
    twsPackagesToExclude.add("edu.iu.dsc.tws.proto");
  }

  public void addJobClass(String jobClass) {
    this.classesToLoad.add(jobClass);
  }

  public boolean excludedPackage(String className) {
    for (String s : this.twsPackagesToExclude) {
      if (className.contains(s)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    if (!name.startsWith("java.")
        && !name.startsWith("sun.")
        && !twsClassesToExclude.contains(name)
        && !this.excludedPackage(name)) {
      InputStream is = getResourceAsStream(name.replace(".", "/") + ".class");
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int readBytes;
        while ((readBytes = is.read(buffer)) > 1) {
          baos.write(buffer, 0, readBytes);
        }
        byte[] bytes = baos.toByteArray();
        return defineClass(name, bytes, 0, bytes.length);
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error loading " + name, e);
        throw new Twister2RuntimeException(e);
      }
    } else {
      return super.loadClass(name);
    }
  }
}
