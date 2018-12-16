# Mesos Uploader Configuration



**twister2.uploader.directory**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td><br/>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.</td></table>

**twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td></td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>"--chmod=+rwx"</td><tr><td>description</td><td><br/>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td><br/>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td><br/>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td><br/>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

