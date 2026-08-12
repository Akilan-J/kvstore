#!/usr/bin/env bash
# Compiles everything into build/. No external dependencies, so plain javac is
# enough - there is no Maven or Gradle here on purpose.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build
javac -Xlint:all -d build $(find src -name '*.java')
echo "built $(find build -name '*.class' | wc -l) classes into build/"
