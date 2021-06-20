cmake_minimum_required(VERSION 3.0)
project(DLevelDB)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake_modules")

file(GLOB CXX_EXEC "src/*.cpp")

add_library(DLevelDB SHARED ${CXX_EXEC})
target_include_directories(DLevelDB PUBLIC inc/)
