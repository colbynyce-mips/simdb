#!/usr/bin/sh

mkdir -p debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
make regress
cd ..
rm -f compile_commands.json
ln -s release/compile_commands.json
