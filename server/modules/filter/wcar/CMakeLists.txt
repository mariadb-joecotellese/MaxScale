add_library(wcar-core
  wcarinmemorystorage.cc
  wcarsqlitestorage.cc
)

add_library(wcar SHARED
  wcar.cc
  wcarconfig.cc
  wcarfilter.cc
  wcarfiltersession.cc
  wcarrecorder.cc
)

target_link_libraries(wcar-core maxscale-common)
target_link_libraries(wcar wcar-core maxscale-common)
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar core)
