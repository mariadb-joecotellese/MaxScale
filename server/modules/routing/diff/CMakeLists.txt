add_library(diff SHARED
  diff.cc
  diffbackend.cc
  diffcommands.cc
  diffconfig.cc
  diffexporter.cc
  diffhistogram.cc
  diffregistry.cc
  diffresult.cc
  diffrouter.cc
  diffroutersession.cc
  diffstats.cc
  diffutils.cc
  )

target_link_libraries(diff maxscale-common)
set_target_properties(diff PROPERTIES VERSION "1.0.0"  LINK_FLAGS -Wl,-z,defs)
install_module(diff enterprise)
