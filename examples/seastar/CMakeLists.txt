cmake_minimum_required(VERSION 3.13)

project(rpc_server CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

# Seastar source and build-tree locations. Override with -DSEASTAR_SOURCE_DIR=...
# / -DSEASTAR_BUILD_DIR=... or the SEASTAR_SOURCE_DIR / SEASTAR_BUILD_DIR
# environment variables.
if(NOT SEASTAR_SOURCE_DIR)
  if(DEFINED ENV{SEASTAR_SOURCE_DIR})
    set(SEASTAR_SOURCE_DIR "$ENV{SEASTAR_SOURCE_DIR}")
  else()
    set(SEASTAR_SOURCE_DIR "$ENV{HOME}/Projects/seastar")
  endif()
endif()
if(NOT SEASTAR_BUILD_DIR)
  if(DEFINED ENV{SEASTAR_BUILD_DIR})
    set(SEASTAR_BUILD_DIR "$ENV{SEASTAR_BUILD_DIR}")
  else()
    set(SEASTAR_BUILD_DIR "${SEASTAR_SOURCE_DIR}/build/debug")
  endif()
endif()
set(SEASTAR_SOURCE_DIR "${SEASTAR_SOURCE_DIR}" CACHE PATH "Path to the Seastar source tree")
set(SEASTAR_BUILD_DIR "${SEASTAR_BUILD_DIR}" CACHE PATH "Path to the configured Seastar build tree (containing SeastarConfig.cmake)")

# Point CMake at the Seastar build tree so find_package(Seastar) can locate
# SeastarConfig.cmake / SeastarTargets.cmake.
list(APPEND CMAKE_PREFIX_PATH "${SEASTAR_BUILD_DIR}")
# SeastarConfig.cmake also pulls in cmake/SeastarDependencies.cmake, which
# only lives in the Seastar source tree (not the build tree).
list(APPEND CMAKE_MODULE_PATH "${SEASTAR_SOURCE_DIR}/cmake")

find_package(Seastar REQUIRED)

add_executable(rpc_server rpc_server.cc)
target_link_libraries(rpc_server PRIVATE Seastar::seastar)

add_executable(rpc_client rpc_client.cc)
target_link_libraries(rpc_client PRIVATE Seastar::seastar)
