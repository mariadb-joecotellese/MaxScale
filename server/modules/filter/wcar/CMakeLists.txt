#include(${CMAKE_SOURCE_DIR}/cmake/BuildBoost.cmake)

add_library(wcar-core
  capinmemorystorage.cc
  capsqlitestorage.cc
  capbooststorage.cc
)

add_library(wcar SHARED
  cap.cc
  capconfig.cc
  capfilter.cc
  capfiltersession.cc
  caprecorder.cc
)

target_link_libraries(wcar-core maxscale-common)
target_link_libraries(wcar wcar-core maxscale-common boost_serialization tbb)
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar core)

add_subdirectory(player)
