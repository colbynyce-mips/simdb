#!/usr/bin/sh

mkdir -p debug
cd debug
rm -f build_debug.log
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. 2>&1 | tee build_debug.log
make clean 2>&1 | tee build_debug.log
make regress 2>&1 | tee build_debug.log
cd ..
rm -f compile_commands.json
ln -s debug/compile_commands.json
rm -f build.log
ln -s debug/build_debug.log build.log
