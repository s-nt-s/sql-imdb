#!/bin/bash
mkdir -p out/
tar -czf out/imdb.tar.gz --transform='s!.*/!!' *.sqlite log/*.log
ls -lah out/