#!/bin/bash

ME=`basename "$0"`
CMD=$1

case $CMD in
    (init)
        # re-initialize the monetdb database
        ./peel.sh db:initialize --connection=monetdb --force
        ;;
    (import)
        # re-import the suites
        RESULT_DIRS=`find results -mindepth 1 -maxdepth 1 -type d | grep -v monetdb | cut -c9-`
        for d in $RESULT_DIRS; do ./peel.sh db:import $d --connection=monetdb --force; done
        ;;
    (extract)
        # re-extract result archive suites
        RESULT_ARCS=`find results -mindepth 1 -maxdepth 1 -type f -name "*.tar.gz" | grep -v monetdb | cut -c9-`
        for a in $RESULT_ARCS; do (cd results; tar -xzvf $a); done
        ;;
    (*)
        echo "Usage: $ME (init|import|extract)"
        exit 1
        ;;
esac
