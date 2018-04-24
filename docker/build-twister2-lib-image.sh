# Build and upload twister2-lib image to Docker Hub
#
# unpack twister2-client.tar.gz file to $TWISTER2_HOME/tmp directory
# build docker image
# tag it
# upload it to Docker Hub

# got to tmp directory in project directory
cd ../tmp

# remove previous copy of tar.gz file
rm twister2-client.tar.gz

# copy new tar.gz file
cp ../bazel-bin/scripts/package/twister2-client.tar.gz .

# untar the package
tar xf twister2-client.tar.gz 

# change dir to main project dir
cd ..

#################
# build docker image with name "twister2-lib" and version "v0.1"
docker build -t twister2-lib:v0.1 -f docker/twister2-lib/Dockerfile .

# Tag the image using docker tag
docker tag twister2-lib:v0.1 auyar/twister2-lib:v0.1

#push image to docker hub,
docker push auyar/twister2-lib:v0.1

