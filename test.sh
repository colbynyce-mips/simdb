#!/usr/bin/sh

target="${1:-release}"
case $target in
    release|debug) echo "Building in $target mode" ;;
    *) echo "Build target must be release or debug" && exit 1 ;;
esac

if [ "$target" = "release" ]; then
    cmake_build_type=Release
else
    cmake_build_type=Debug
fi

mkdir -p $target
cd $target
rm -f build.log
cmake -DCMAKE_BUILD_TYPE=$cmake_build_type -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. 2>&1 | tee build.log
make clean 2>&1 | tee build.log
make regress 2>&1 | tee build.log
cd ..
rm -f compile_commands.json
ln -s $target/compile_commands.json
rm -f build.log
ln -s $target/build.log
