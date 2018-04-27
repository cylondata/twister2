# image name and version: twister2:v0.01 
# this docker image is the base image for Twister2 image
# It has the library jar files from other projects
# It does not have the jar files from Twister2 team
# Main purpose of this image is to serve as a base image to Twister2 image
# This image is uploaded to docker hub and when twister2 image is build frequently, 
# they don't need to be uploaded to docker hub with each update

# initial steps:
#   build twister2 project
#   unpack it at twister2/tmp directory

###########################################
# Building docker image
###########################################

# build docker image with name "twister2-lib" and version "v0.1"
# this command has to be executed from twister2 directory
docker build -t twister2-lib:v0.1 -f docker/twister2-lib/Dockerfile .

###########################################
# Uploading docker image to docker hub
###########################################

# login to docker hub, enter login name and pwd when asked
docker login

# Tag the image using docker tag
docker tag twister2-lib:v0.1 auyar/twister2-lib:v0.1

#push image to docker hub,
docker push auyar/twister2-lib:v0.1

