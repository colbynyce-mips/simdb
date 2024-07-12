#!/usr/bin/sh

mkdir -p release
cd release
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
make regress
cd ..
rm -f compile_commands.json
ln -s release/compile_commands.json
