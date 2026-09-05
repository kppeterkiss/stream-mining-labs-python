#!/usr/bin/env bash

if [[ -z "${JAVA_HOME:-}" ]]; then
  export JAVA_HOME="$('/usr/libexec/java_home')"
fi

export PYTHONPATH="src"

echo "JAVA_HOME=$JAVA_HOME"
echo "PYTHONPATH=$PYTHONPATH"
