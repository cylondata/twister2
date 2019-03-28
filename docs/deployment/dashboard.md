# Dashboard

Twister2 dashboard gives you a comprehensive overview of all your jobs running in a cluster. Dashboard comes integrated with twister2's main binary build, but it runs as a separate and standalone process from twister2 framework. Due to this nature, you should manually start the dashboard, if required.

## Starting dashboard

To start the dashboard, execute following command from the root directory.

```bash
./bin/twister2 dash
```

## Configuring dashboard

Since dashboard runs as an independent component, it has a separate set of configuration files located at ```conf/dashboard```

### application.properties

This is a typical spring boot configuration file, where you can tune bunch of web & database server related parameters.

#### Datasource

Twister2 by default uses a file based H2 database to persist dashboard related data. However, you are free to configure any other database that suits your requirements and environment. If you are planning to use a different database, make sure to [recompile dashboard server](../compiling/compile-dashboard.md) with necessary drivers.

For more on datasource related configurations, refer [spring datasource documentation.](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-sql.html)

#### Webserver

Dashboard process spawns an embedded tomcat instance on port 8080.
How ever, this can be configured to use any other port. Refer [spring documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/howto-properties-and-configuration.html) for more information.

### log4j2.xml

Dashboard uses log4J2 for logging. By default, it logs to both Console and File. You may change this behaviour as per [log4j2 documentation](https://logging.apache.org/log4j/2.x/manual/).


