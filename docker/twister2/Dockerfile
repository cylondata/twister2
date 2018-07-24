# FROM java:8-jre
# FROM openjdk:8-jre-alpine
FROM auyar/twister2-lib:v0.1

COPY tmp/twister2-dist/lib/libresource-scheduler-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libcommon-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libapi-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libconfig-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libmaster-java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libproto_job_java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libproto_resource_scheduler_java.jar /twister2/lib/
COPY tmp/twister2-dist/lib/libproto_network_java.jar /twister2/lib/

ENV CLASSPATH="lib/*"

WORKDIR /twister2/

#CMD ["java", "edu.iu.dsc.tws.rsched.schedulers.k8s.worker.KubernetesWorkerStarter"]
