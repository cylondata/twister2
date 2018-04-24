# docker image name and version: twister2:v0.01 
# twister2 project docker image

# init steps
#   build twister2 project
#   unpack it at twister2/tmp directory

###########################################
# Building the docker image
###########################################

# build docker image with name "twister2" and version "v0.01"
# this command has to be executed from twister2 directory
docker build -t twister2:v0.01 -f docker/twister2/Dockerfile .

###########################################
# Uploading the docker image to Docker Hub
###########################################

# login to docker hub, enter login name and pwd when asked
docker login

# Tag the image using docker tag
docker tag twister2:v0.01 auyar/twister2:v0.01

#push image to docker hub,
docker push auyar/twister2:v0.01

###########################################
# Running docker image to check, if desired
# These two does not make much sense
# maybe for some testing
###########################################

# run the image locally, if you want. Optional. 
docker run twister2:v0.01

#run the container with the image from docker hub, probably in another machine
docker run auyar/twister2:v0.01

