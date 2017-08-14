#!/bin/bash

# Generates an IntelliJ project in twister2

set -o errexit
cd $(dirname "$0")
echo "changing to `pwd`/.."
cd ..
mkdir -p .idea/
cp -R scripts/resources/idea/* .idea/
source scripts/get_all_twister2_paths.sh
echo "Generating IDEA project..."

readonly iml_file=twister2.iml
# Code generated by AutoValue is put in $MODULE_DIR/out/generated; adding it as a Source Root
# allows IntelliJ to find it while editing. (that is, generated classes won't be marked as unknown.)
cat > $iml_file <<EOH
<?xml version="1.0" encoding="UTF-8"?>
<module type="JAVA_MODULE" version="4">
  <component name="FacetManager">
    <facet type="Python" name="Python">
      <configuration sdkName="Python 2.7.10 (/usr/bin/python)" />
    </facet>
  </component>
  <component name="NewModuleRootManager">
    <orderEntry type="sourceFolder" forTests="false" />
    <output url="file://\$MODULE_DIR\$/out" />
EOH

# Find all top-level dirs that should be parsed for possible source/tests
for content_dir in `find . -maxdepth 1 -type d -path './[^.]*' | cut -d '/' -f 2 | grep -v out`; do

  if [ "$content_dir" == "twister2" ]; then

  cat >> $iml_file <<EOH
    <content url="file://\$MODULE_DIR$/twister2">
EOH
  echo '      <sourceFolder url="file://$MODULE_DIR$/twister2/config/src" type="java-resource" />'>> $iml_file
  twister2_java_paths="$(get_twister2_source_paths)"

  for source in ${twister2_java_paths}; do
    if [[ $source == *"javatests" ]]; then
      is_test_source="true"
    elif [[ $source == *"tests/"* ]]; then
      is_test_source="true"
    else
      is_test_source="false"
    fi
    folderType="sourceFolder";
    if [[ -f "$source/xxBUILD" ]]; then
      folderType="excludeFolder"
      echo '      <excludeFolder url="file://$MODULE_DIR$/'"${source}\"  />" >> $iml_file
    else
      echo '      <sourceFolder url="file://$MODULE_DIR$/'"${source}\" isTestSource=\"${is_test_source}\" />" >> $iml_file
    fi
  done

  else
    echo "    <content url=\"file://\$MODULE_DIR$/${content_dir}\">"  >> $iml_file
  fi
  cat >> $iml_file <<'EOF'
    </content>
EOF
done

# Write a module-library entry, usually a jar file but occasionally a directory.
function write_jar_entry() {
  local root_file=$1
  if [[ $# > 1 ]]; then
    local source_path=$2
  else
    local source_path=""
  fi
  local protocol="file"
  local file_end=""
  if [[ $root_file == *.jar ]]; then
    protocol="jar"
    file_end="!"
  fi
  local  libfile="\$MODULE_DIR\$/${root_file}"
  if [[ "$root_file" = /* ]]; then
    libfile="${root_file}"
  fi
  local readonly basename=${root_file##*/}
    cat >> $iml_file <<EOF
      <orderEntry type="module-library">
        <library name="${basename}">
          <CLASSES>
            <root url="${protocol}://${libfile}${file_end}/" />
          </CLASSES>
          <JAVADOC />
EOF
  if [[ -z "${source_path}" ]]; then
    echo "          <SOURCES />" >> $iml_file
  else
    cat >> $iml_file <<EOF
          <SOURCES>
            <root url="jar:/\$MODULE_DIR\$/${source_path}!/" />
          </SOURCES>
EOF
  fi
  if [[ $protocol == "file" ]]; then
    cat >> $iml_file <<EOF
          <jarDirectory url="file://\$MODULE_DIR\$/${root_file}" recursive="false" />
EOF
  fi
  cat >> $iml_file <<'EOF'
      </library>
    </orderEntry>
EOF
}

# Slight hack to make sure (1) our langtools is picked up before the SDK
# default, but that (2) SDK is picked up before auto-value, because that
# apparently causes problems for auto-value otherwise.
readonly javac_jar="third_party/java/jdk/langtools/javac.jar"
write_jar_entry "$javac_jar"

cat >> $iml_file <<'EOF'
    <orderEntry type="inheritedJdk" />
EOF
twister2_thirdparty_deps="$(get_twister2_thirdparty_dependencies)"
for jar in ${twister2_thirdparty_deps}; do
  if [[ jar != "$javac_jar" ]]; then
    write_jar_entry $jar
  fi
done

for path_pair in ${GENERATED_PATHS}; do
  write_jar_entry ${path_pair//:/ }
done
twister2_resolved_deps="$(get_twister2_bazel_deps)"

for jar in ${twister2_resolved_deps}; do
  write_jar_entry $jar
done
#<orderEntry type="library" name="proto" level="application" />
twister2_binary_paths="$(collect_generated_binary_deps)"

for jar in ${twister2_binary_paths}; do
  write_jar_entry "$jar";
done
#write_jar_entry "bazel-bin/twister2/metricsmgr/src/thrift"

cat >> $iml_file <<'EOF'
    <orderEntry type="library" name="Python 2.7.10 (/usr/bin/python) interpreter library" level="application" />
  </component>
</module>
EOF

echo Done. IDEA module file: $iml_file

IDEA=`ls -1d /Applications/IntelliJ\ * 2> /dev/null| tail -n1`
if [ -n "$IDEA" ]; then
  echo "Opening Heron project in IDEA..."
  open -a "$IDEA" .
  echo "Done."
else
  echo
  echo "Could not locate IntelliJ IDEA. Manually open `pwd` as a project in IDEA."
fi
