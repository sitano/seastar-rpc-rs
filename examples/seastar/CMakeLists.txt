cmake_minimum_required(VERSION 3.13)

project(rpc_server CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Seastar build mode, as in Seastar's `configure.py --mode`. The build type
# must match Seastar's: its exported targets pick ABI-relevant defines
# (SEASTAR_DEBUG, SEASTAR_DEFAULT_ALLOCATOR, ...) by $<CONFIG>.
set(SEASTAR_MODE "debug" CACHE STRING "Seastar build mode: release, debug, dev, sanitize or fuzz")
set(_seastar_modes release debug dev sanitize fuzz)
set_property(CACHE SEASTAR_MODE PROPERTY STRINGS ${_seastar_modes})
list(FIND _seastar_modes "${SEASTAR_MODE}" _seastar_mode_index)
if(_seastar_mode_index EQUAL -1)
  message(FATAL_ERROR "SEASTAR_MODE=${SEASTAR_MODE} is not one of: ${_seastar_modes}")
endif()

if(NOT CMAKE_BUILD_TYPE)
  # Same mapping as MODE_TO_CMAKE_BUILD_TYPE in Seastar's configure.py.
  set(_seastar_build_types RelWithDebInfo Debug Dev Sanitize Fuzz)
  list(GET _seastar_build_types ${_seastar_mode_index} CMAKE_BUILD_TYPE)
endif()

# Seastar lookup order:
#   1. An explicit build tree: -DSEASTAR_BUILD_DIR=... or the SEASTAR_BUILD_DIR
#      environment variable (plus optional SEASTAR_SOURCE_DIR; defaults to
#      two levels above the build dir).
#   2. Standard find_package() discovery: Seastar_DIR, CMAKE_PREFIX_PATH,
#      system install prefixes.
#   3. A local checkout at $HOME/Projects/seastar/build/<SEASTAR_MODE>.
set(SEASTAR_SOURCE_DIR "$ENV{SEASTAR_SOURCE_DIR}" CACHE PATH "Path to the Seastar source tree (build-tree usage only)")
set(SEASTAR_BUILD_DIR "$ENV{SEASTAR_BUILD_DIR}" CACHE PATH "Path to a configured Seastar build tree (containing SeastarConfig.cmake)")

# A Seastar build tree (unlike an install) needs cmake/SeastarDependencies.cmake
# and the Find*.cmake modules from the source tree.
macro(use_seastar_build_tree src build)
  list(PREPEND CMAKE_PREFIX_PATH "${build}")
  list(APPEND CMAKE_MODULE_PATH "${src}/cmake")
endmacro()

if(SEASTAR_BUILD_DIR)
  if(NOT SEASTAR_SOURCE_DIR)
    get_filename_component(SEASTAR_SOURCE_DIR "${SEASTAR_BUILD_DIR}/../.." ABSOLUTE)
  endif()
  use_seastar_build_tree("${SEASTAR_SOURCE_DIR}" "${SEASTAR_BUILD_DIR}")
  set(Seastar_DIR "${SEASTAR_BUILD_DIR}")
  find_package(Seastar REQUIRED CONFIG NO_DEFAULT_PATH NO_CMAKE_PACKAGE_REGISTRY)
else()
  # Skip the user package registry (~/.cmake/packages): every Seastar build
  # tree registers itself there, and build trees need the source-tree module
  # path set up first (see use_seastar_build_tree).
  #
  # Seastar_DIR (cached by a previous run or passed by hand) may point at a
  # build tree too; take its source dir from the build tree's own cache.
  if(Seastar_DIR AND EXISTS "${Seastar_DIR}/CMakeCache.txt")
    load_cache("${Seastar_DIR}" READ_WITH_PREFIX _seastar_ CMAKE_HOME_DIRECTORY)
    use_seastar_build_tree("${_seastar_CMAKE_HOME_DIRECTORY}" "${Seastar_DIR}")
  endif()
  find_package(Seastar QUIET NO_CMAKE_PACKAGE_REGISTRY)
  if(NOT Seastar_FOUND)
    unset(Seastar_DIR CACHE)
    set(_seastar_src "$ENV{HOME}/Projects/seastar")
    use_seastar_build_tree("${_seastar_src}" "${_seastar_src}/build/${SEASTAR_MODE}")
    find_package(Seastar REQUIRED NO_CMAKE_PACKAGE_REGISTRY)
  endif()
endif()
message(STATUS "Using Seastar from ${Seastar_DIR}")

add_executable(rpc_server rpc_server.cc)
target_link_libraries(rpc_server PRIVATE Seastar::seastar)

add_executable(rpc_client rpc_client.cc)
target_link_libraries(rpc_client PRIVATE Seastar::seastar)
