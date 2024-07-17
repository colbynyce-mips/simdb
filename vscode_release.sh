#!/usr/bin/sh

mkdir -p release
cd release
rm -f build_release.log
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. 2>&1 | tee build_release.log
make clean 2>&1 | tee build_release.log
make regress 2>&1 | tee build_release.log
cd ..
rm -f compile_commands.json
ln -s release/compile_commands.json
rm -f build.log
ln -s release/build_release.log build.log
