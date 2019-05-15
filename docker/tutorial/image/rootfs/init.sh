#! /bin/sh


echo "starting sshd"
/usr/sbin/sshd -D &


sleep infinity
return_code=$?

echo -n "$return_code" > /dev/termination-log
exit $return_code
