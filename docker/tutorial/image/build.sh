docker build -t twister2tutorial/twister2:standalone .
docker tag twister2tutorial/twister2:standalone
docker push twister2tutorial/twister2:standalone

# download command
# docker pull twister2tutorial/twister2:standalone
# run command
# docker run -it twister2tutorial/twister2:standalone bash