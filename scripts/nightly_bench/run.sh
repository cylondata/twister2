# remove
rm twister2 -r -f
echo "Removing jobs...."
rm /N/u/cwidanage/.twister2/jobs/* -r -f

# clone
git clone https://github.com/DSC-SPIDAL/twister2.git

# build
echo "CDing to twitser2...."
cd twister2 || (echo "Failed to cd" && exit)
bazel clean
echo "Building...."
bazel build --config=ubuntu //scripts/package:tarpkgs --action_env=JAVA_HOME || (echo "Build failed... " && exit)

echo "Twister2 built...."
cd bazel-bin/scripts/package/ || (echo "Build has failed" && exit)

# extract
tar -xvzf twister2-0.5.0-SNAPSHOT.tar.gz

# copy hosts
cd ../../../../
rm twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/nodes -f
cp nodes twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/

rm twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/core.yaml -f
cp core.yaml twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/

rm twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/common/core.yaml -f
cp common_core.yaml twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/common/core.yaml

rm twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/mpi.sh -f
cp mpi.sh twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/

rm twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/resource.yaml -f
cp resource.yaml twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/conf/standalone/

# navigate to test root
cd twister2/bazel-bin/scripts/package/twister2-0.5.0-SNAPSHOT/util/test/

# run tests
TIMEOUT=10800
timeout $TIMEOUT python3 launcher.py comms_bcast
timeout $TIMEOUT python3 launcher.py comms_reduce
timeout $TIMEOUT python3 launcher.py comms_all_gather
timeout $TIMEOUT python3 launcher.py comms_all_reduce
timeout $TIMEOUT python3 launcher.py comms_gather

#return to current location
cd /N/u/cwidanage/auto-t2
node auto.js