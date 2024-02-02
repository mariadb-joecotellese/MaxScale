add_library(comparator SHARED
  comparator.cc
  cbackend.cc
  ccommands.cc
  cconfig.cc
  cexporter.cc
  cregistry.cc
  cresult.cc
  crouter.cc
  csession.cc
  cstats.cc
  )

target_link_libraries(comparator maxscale-common)
set_target_properties(comparator PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(comparator core)
