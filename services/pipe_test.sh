#!/bin/bash

THIS_HOSTNAME=`hostname`
#echo $THIS_HOSTNAME " Running shell script"
while read LINE; do
   echo $THIS_HOSTNAME" "${LINE}
done
