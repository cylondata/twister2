#! /bin/bash
#
# A list of IP addresses are given as a command line parameter list to this script
# This script will try to connect to all those IP addresses with ssh
# It will return when it is able to connect to all IP address with ssh
# It is used to check whether the password free access is established between this machine and the others
# This script is executed before running mpirun command on mpimaster pod
#
# Implementation:
# All IP addresses are saved in a map
# IP addresses are used as keys
# Connection status is used as values: started or notstarted
# when we connected to an IP address successfully, we mark the status as started for that IP
# when all IP addresses are marked as started, the processes finishes with success, exiting with the code 0
#
# To-do: add timeout mechanism
#


##################################################################################
### Initialize the variables
##################################################################################

ALL_STARTED="false"
POLLING_INTERVAL=0.1

# initialize the map
declare -A ipmap
for arg; do
  ipmap[$arg]="notstarted"
done

##################################################################################
### Functions start
##################################################################################
# print the map, test method
print_map(){
  for ip in "${!ipmap[@]}"; do
    echo "$ip: ${ipmap[$ip]}";
  done
}

# check whether the values in the ipmap are all started
have_all_started(){
  for status in "${ipmap[@]}"; do
    if [ $status != "started" ]; then
#      echo "not all started."
      ALL_STARTED="false"
      return 1
    fi
  done
  ALL_STARTED="true"
  echo "all sshd servers running."
  return 0
}

# try to connect to each IP address that is not already connected
# mark the ip addresses that are successfully connected as started
check_connection(){
  for ip in "${!ipmap[@]}"; do
    if [ ${ipmap[$ip]} != "started" ]; then
      # echo "$ip status notstarted: will check."
      ssh $ip echo
      if [ $? == 0 ]; then
        ipmap[$ip]="started"
      fi
#    else
#      echo "$ip status started, no need to check the connection"
    fi
  done
}

#############################################################
# Functions ended
# main script starts
#############################################################

check_connection
have_all_started

while [ $ALL_STARTED != "true" ]
do
  sleep $POLLING_INTERVAL
  check_connection
  have_all_started
done

print_map
echo "exiting with success."
exit 0
