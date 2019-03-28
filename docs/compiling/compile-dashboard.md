# Compiling Dashboard

Twister2 dashboard has two components.

1. Front End Web application; A [ReactJS](https://reactjs.org/) based web application
2. Web server; A [spring boot application](https://spring.io/projects/spring-boot), which exposes a RESTful Web Service (JAX-RS)

Even though (2) Embedded web server, has been included in the main bazel build, (1) Front End Web application should be build separately.

## Prerequisites to build web application

* Node 6 or later : [Download](https://nodejs.org/en/download/)
* NPM 5.2 or later : [How to Setup](https://www.npmjs.com/get-npm)
* SASS : [How to setup](https://sass-lang.com/install)

## Compiling web application

Having all above prerequisites ready, navigate to the root folder of dashboard client.

```cd dashboard/client```

### Building SCSS

Now build SCSS files to produce CSS files with following command

```npm run build-css```

### Building React app

Use below command to build the react application.

```npm run build```

This will create an optimized production build in ```{twister2_root}/dashboard/server/src/main/resources/static``` directory.

### Building Dashboard

As the final step, [run the main twister2 build](./compiling.md), to generate all the binaries including dashboard.
