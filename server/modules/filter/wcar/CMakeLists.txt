#include(${CMAKE_SOURCE_DIR}/cmake/BuildBoost.cmake)

add_library(wcar-core
  capbooststorage.cc
)

add_library(wcar SHARED
  cap.cc
  capconfig.cc
  capfilter.cc
  capfiltersession.cc
  caprecorder.cc
)

target_link_libraries(wcar wcar-core maxscale-common boost_serialization tbb)
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar enterprise)

add_subdirectory(player)
