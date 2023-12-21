# Requires Kafka for the time being.
if (NOT RDKAFKA_FOUND)
  include(${CMAKE_SOURCE_DIR}/cmake/BuildRdkafka.cmake)
  include_directories(${RDKAFKA_INCLUDE_DIR})
endif()

add_library(comparator SHARED
  comparator.cc
  comparatorbackend.cc
  comparatorcommands.cc
  comparatorconfig.cc
  comparatorexporter.cc
  comparatorrouter.cc
  comparatorsession.cc
  )

target_link_libraries(comparator maxscale-common ${RDKAFKA_LIBRARIES})
add_dependencies(comparator librdkafka)
set_target_properties(comparator PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(comparator core)
