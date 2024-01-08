add_library(comparator SHARED
  comparator.cc
  comparatorbackend.cc
  comparatorcommands.cc
  comparatorconfig.cc
  comparatorexporter.cc
  comparatorresult.cc
  comparatorrouter.cc
  comparatorsession.cc
  )

target_link_libraries(comparator maxscale-common)
set_target_properties(comparator PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(comparator core)
