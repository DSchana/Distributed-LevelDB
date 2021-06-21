echo Fetching git submodules
git submodule update --init

echo Building rapidjson for cmake config files
cd DLevelDB/submodules/rapidjson
mkdir build
cd build
cmake ..
make -j install
