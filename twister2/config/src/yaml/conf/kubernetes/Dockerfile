# FROM java:8-jre
FROM openjdk:8-jre-alpine

COPY lib/commons-compress-1.15.jar /twister2/lib/
COPY lib/libresource-scheduler-java.jar /twister2/lib/
COPY lib/libcommon-java.jar /twister2/lib/
COPY lib/libapi-java.jar /twister2/lib/
COPY lib/libconfig-java.jar /twister2/lib/
COPY lib/libproto_job_java.jar /twister2/lib/
COPY lib/protobuf-java-3.4.0.jar /twister2/lib/
COPY lib/libproto_resource_scheduler_java.jar /twister2/lib/
COPY lib/snakeyaml-1.15.jar /twister2/lib/

ENV CLASSPATH="lib/libresource-scheduler-java.jar:lib/commons-compress-1.15.jar:lib/libcommon-java.jar:lib/libapi-java.jar:lib/libconfig-java.jar:lib/libproto_job_java.jar:lib/protobuf-java-3.4.0.jar:lib/libproto_resource_scheduler_java.jar:lib/snakeyaml-1.15.jar"

WORKDIR /twister2/

CMD ["java", "edu.iu.dsc.tws.rsched.schedulers.k8s.worker.KubernetesWorker"]
