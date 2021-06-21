echo Building Distributed Level DB
mkdir build
cd build
cmake ..
make -j all

echo Creating config files
cp -r ../.config.default ./.config
