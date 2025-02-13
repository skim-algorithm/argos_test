#!/bin/bash
containers=$1
worker=$2
IFS=','
read -r -a array <<< "${containers}"
for element in "${array[@]}"
do
    if [ "up" = "$worker" ]; then
        execCmd="docker-compose -f \"docker-compose.yml\" stop $element"
        eval $execCmd
        execCmd="docker-compose -f \"docker-compose.yml\" up -d $element"
        eval $execCmd
    elif [ "down" = "$worker" ]; then
        execCmd="docker-compose -f \"docker-compose.yml\" stop $element"
        eval $execCmd
    fi    
done

