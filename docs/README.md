# Documentation Guide

This documentation is written using mark down files. These markdown files are converted into HTML and deployed on to github pages.

In order to write documentation one has to be knowledgeable about markdown files. 

Documentation is built using [Docusaurus](https://docusaurus.io). Please refer its documentation on how to build.

## Deploying the documentation to website

The website created using markdown files are deployed onto Github pages. We use a custom URL for the website.

## Installing Docusaurus

Lets briefly look at how to install Docusaurus. This step is only needed if you would like to update the deployed website.

### Install NodeJs

Follow the installtion instructions []Nodejs](https://nodejs.org/en/download/) and [Linux NodeJs](https://github.com/nodesource/distributions/blob/master/README.md)

Here is how to install on Ubuntu or Debian

```bash
# Using Ubuntu
curl -sL https://deb.nodesource.com/setup_11.x | sudo -E bash -
sudo apt-get install -y nodejs

# Using Debian, as root
curl -sL https://deb.nodesource.com/setup_11.x | bash -
apt-get install -y nodejs
```

### Install Yarn

Follow the instructions at [Yarn Install](https://yarnpkg.com/en/docs/install#debian-stable)

```bash
curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list

sudo apt-get update && sudo apt-get install yarn
```

### Install Docusaurus

```bash
sudo npm install -g docusaurus --unsafe-perm=true --allow-root

yarn add @babel/plugin-proposal-class-properties --dev
```

### How to deploy

Do the changes to the documents

```bash
cd website
yarn run build
GIT_USER=<GIT_USER> CURRENT_BRANCH=master USE_SSH=false yarn run publish-gh-pages
```

## Writing Documentation

You can add your documentation to the docs folder as a markdown file. 

On top of the markdown file add a header like following to link the markdown file to other files and use it in the sidebar. The id is used to refer to the document in the sidebar.

```text
id: img_proc
title: Image Processing
```

Inside website folder there is a file called, sidebars.json. You can modify that file to include the documentation in the sidebar.