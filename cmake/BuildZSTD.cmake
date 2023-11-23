# Build the ZSTD library from source

set(ZSTD_REPO "https://github.com/facebook/zstd.git" CACHE STRING "ZSTD Git repository")

set(ZSTD_TAG "v1.5.5" CACHE STRING "ZSTD Git tag")

message(STATUS "Using zstd version ${ZSTD_TAG}")

set(ZSTD_BASE "${CMAKE_BINARY_DIR}/zstd")

set(ZSTD_SOURCE "${ZSTD_BASE}/src")
set(ZSTD_BINARY "${ZSTD_BASE}/buildir")

ExternalProject_Add(zstd
  GIT_REPOSITORY ${ZSTD_REPO}
  GIT_TAG ${ZSTD_TAG}
  GIT_SHALLOW TRUE
  SOURCE_DIR ${ZSTD_SOURCE}
  CMAKE_ARGS ../src/build/cmake -DCMAKE_BUILD_TYPE:STRING=RelWithDebInfo -DZSTD_BUILD_SHARED=OFF -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_LEGACY_SUPPORT=OFF -DZSTD_MULTITHREAD_SUPPORT=ON -Wno-dev
  BINARY_DIR ${ZSTD_BINARY}
  BUILD_COMMAND make
  INSTALL_COMMAND ""
  UPDATE_COMMAND ""
  LOG_DOWNLOAD 1
  LOG_UPDATE 1
  LOG_CONFIGURE 1
  LOG_BUILD 1
  LOG_INSTALL 1)

set(ZSTD_INCLUDE_DIRS ${ZSTD_SOURCE}/lib CACHE PATH "ZSTD headers" FORCE)
set(ZSTD_LIBRARIES ${ZSTD_BINARY}/lib/libzstd.a CACHE PATH "ZSTD library" FORCE)
