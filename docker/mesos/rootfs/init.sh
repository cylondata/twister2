#! /bin/sh

#touch twister2/deneme.txt
#ls -al | tee twister2/deneme.txt
#echo $(ls twister2/)

cd twister2
if [ "$DOWNLOAD_METHOD" = 'HTTP' ]; then  
	wget 149.165.150.81:8082/twister2/mesos/twister2-core-0.2.1.tar.gz
	wget 149.165.150.81:8082/twister2/mesos/twister2-job.tar.gz
else 
	echo "hdfs or other method was choosen"
fi

#export LD_LIBRARY_PATH="/openmpi-build/lib"

echo "starting sshd"
/usr/sbin/sshd -D &

#chmod 600 ~/.ssh/id_rsa

if [ ! -f twister2-core-0.2.1.tar.gz ]; then
    echo "file not found. Probably could not download the file"
else
    tar xvf twister2-core-0.2.1.tar.gz
    tar xvf twister2-job.tar.gz
    echo "files fetched and unpacked"
    java -cp twister2-core/lib/*:twister2-job/libexamples-java.jar:/customJars/* $CLASS_NAME
fi

#if [ $WORKER_ID -eq 1 ] || [ $WORKER_ID -eq 0 ]
#then
#    tar xvf twister2-core.tar.gz
#    tar xvf twister2-job.tar.gz
#    echo "files fetched and unpacked"
#    java -cp twister2-core/lib/*:twister2-job/libexamples-java.jar:/customJars/* $CLASS_NAME
#else 
#    tar xvf twister2-core.tar.gz
#    tar xvf twister2-job.tar.gz
#    echo "files fetched and unpacked"
#fi

#remove customJars after manage to add that jars to core.tar.gz


sleep infinity
return_code=$?

echo -n "$return_code" > /dev/termination-log
exit $return_code
