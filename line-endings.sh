#!/bin/bash

for file in $(ls *.sh)
do 
  vi +':w ++ff=unix' +':q' ${file}
done

for file in $(ls *.sh)
do 
  chmod +x  ${file}
done
