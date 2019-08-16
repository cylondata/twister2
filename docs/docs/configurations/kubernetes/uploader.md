# Kubernetes Uploader Configuration



### When a job is submitted, the job package needs to be transferred to worker pods<br/>Two upload methods are provided:<br/>  a) Job Package Transfer Using kubectl file copy (default)<br/>  b) Job Package Transfer Through a Web Server<br/>Following two configuration parameters control the uploading with the first method<br/>when the submitting client uploads the job package directly to pods using kubectl copy
**twister2.kubernetes.client.to.pods.uploading**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if the value of this parameter is true,<br/>the job package is transferred from submitting client to pods directly<br/>if it is false, the job package will be transferred to pods through the upload web server<br/>default value is true</td></table>

**twister2.kubernetes.uploader.watch.pods.starting**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>When the job package is transferred from submitting client to pods directly,<br/>upload attempts can either start after watching pods or immediately when StatefulSets are created<br/>watching pods before starting file upload attempts is more accurate<br/>it may be slightly slower to transfer the job package by watching pods though<br/>default value is true</td></table>

### Following configuration parameters sets up the upload web server,<br/>when the job package is transferred through a webserver<br/>Workers download the job package from this web server
**twister2.uploader.directory**
<table><tr><td>default</td><td>"/absolute/path/to/uploder/directory/"</td><tr><td>description</td><td>the directory where the job package file will be uploaded,<br/>make sure the user has the necessary permissions to upload the file there.<br/>full directory path to upload the job package with scp</td></table>

**twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/absolute/path/to/uploder/directory/"</td><tr><td>description</td><td></td></table>

**twister2.download.directory**
<table><tr><td>default</td><td>"http://webserver.address:port/download/dir/path"</td><tr><td>description</td><td>web server link of the job package download directory<br/>this is the same directory as the uploader directory</td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>"--chmod=+rwx"</td><tr><td>description</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"user@uploadserver.address"</td><tr><td>description</td><td>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"user@uploadserver.address"</td><tr><td>description</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

