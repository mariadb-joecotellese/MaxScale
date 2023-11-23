# Requires Kafka for the time being.
if (NOT RDKAFKA_FOUND)
  include(${CMAKE_SOURCE_DIR}/cmake/BuildRdkafka.cmake)
  include_directories(${RDKAFKA_INCLUDE_DIR})
endif()

add_library(urat SHARED
  urat.cc
  uratbackend.cc
  uratcommands.cc
  uratconfig.cc
  uratexporter.cc
  uratrouter.cc
  uratsession.cc
  )

target_link_libraries(urat maxscale-common ${RDKAFKA_LIBRARIES})
add_dependencies(urat librdkafka)
set_target_properties(urat PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(urat core)
