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
