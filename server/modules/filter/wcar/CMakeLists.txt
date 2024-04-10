if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND ${CMAKE_CXX_COMPILER_VERSION} VERSION_GREATER_EQUAL 9.1.0)
  message(STATUS "Using <execution> for parallel sorting")
  add_definitions(-DHAVE_STD_EXECUTION=1)
else()
  message(STATUS "Compiler too old or <execution> not found, using serial sorting")
endif()

include(${CMAKE_SOURCE_DIR}/cmake/BuildBoost.cmake)

add_library(wcar-core
  capbooststorage.cc
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

target_link_libraries(wcar wcar-core maxscale-common boost_serialization tbb)
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar enterprise)

add_subdirectory(player)
add_subdirectory(report)
