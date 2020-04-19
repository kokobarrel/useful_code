#!/bin/bash


find . -name "*.java" -exec perl -p -i -e 's/ Log\.v/ \/\/Log\.v/g' {} \;

