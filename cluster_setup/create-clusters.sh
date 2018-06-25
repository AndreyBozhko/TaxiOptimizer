#!/bin/bash

for yamlfile in `find $PWD/*/*.yml` ; do
  peg up $yamlfile
  wait
done


# for each cluster, there should be master.yml and workers.yml files in the corresponding folder
