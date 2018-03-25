# twister2:v0.01 gets job package, unpacks it, loads it, starts user container, waits indefinetely sleeping ...

# init steps
#   build twister2 project
#   unpack it at tmp directory
# copy Dockerfile to twister2-dist directory
# follow below steps in twister2-dist directory

###########################################
# Building docker image
###########################################

# build docker image with name "twister2" and version "v0.01"
docker build -t twister2:v0.01 .

# run the image
docker run twister2:v0.01

###########################################
# Uploading docker image to docker hub
###########################################

# login to docker hub, enter login name and pwd when asked
docker login

# Tag the image using docker tag
docker tag twister2:v0.01 auyar/twister2:v0.01

#push image to docker hub,
docker push auyar/twister2:v0.01

#run container with the image from docker hub, probably in another machine
docker run auyar/twister2:v0.01


