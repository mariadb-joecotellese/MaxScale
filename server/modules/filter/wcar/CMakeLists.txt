# RHEL 8 does not have the <execution> header and TBB on RHEL 7 is too old.
set(CMAKE_REQUIRED_LINK_OPTIONS "-ltbb")
check_cxx_source_compiles("
#include <execution>
#include <algorithm>
int main() {
  int a[100]{5, 4, 3, 2, 1, 0};
  std::sort(std::execution::par, std::begin(a), std::end(a));
  return a[0];
}" HAVE_STD_EXECUTION)

if(HAVE_STD_EXECUTION)
  add_definitions(-DHAVE_STD_EXECUTION=1)
else()
  message(STATUS "Compiler too old or <execution> not found, using serial sorting")
endif()

include(${CMAKE_SOURCE_DIR}/cmake/BuildBoost.cmake)

add_library(wcar-core
  capbooststorage.cc
  capquerysort.cc
)
add_dependencies(wcar-core maxscale-common)

add_library(wcar SHARED
  cap.cc
  capconfig.cc
  capfilter.cc
  capfiltersession.cc
  capsessionstate.cc
  caprecorder.cc
)

target_link_libraries(wcar wcar-core maxscale-common boost_serialization)
if (HAVE_STD_EXECUTION)
  target_link_libraries(wcar tbb)
endif()
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar enterprise)

add_subdirectory(player)
add_subdirectory(report)
add_subdirectory(test)
