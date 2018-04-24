# FROM java:8-jre
# FROM openjdk:8-jre-alpine
FROM auyar/twister2-lib:v0.1

COPY tmp/twister2-dist/lib/libresource-scheduler-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libcommon-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libapi-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libconfig-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libproto_job_java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libproto_resource_scheduler_java.jar /twister2/lib/

ENV CLASSPATH="lib/libresource-scheduler-java.jar:lib/commons-compress-1.15.jar:lib/libcommon-java.jar:lib/libapi-java.jar:lib/libconfig-java.jar:lib/libproto_job_java.jar:lib/protobuf-java-3.4.0.jar:lib/libproto_resource_scheduler_java.jar:lib/snakeyaml-1.15.jar:lib/client-java-1.0.0-beta2.jar:lib/client-java-api-1.0.0-beta2.jar:lib/swagger-annotations-1.5.12.jar:lib/gson-2.6.2.jar:lib/okhttp-2.7.5.jar:lib/log4j-1.2.17.jar:lib/joda-time-2.9.3.jar:lib/commons-codec-1.10.jar:lib/okio-1.6.0.jar"

WORKDIR /twister2/

CMD ["java", "edu.iu.dsc.tws.rsched.schedulers.k8s.worker.KubernetesWorker"]
