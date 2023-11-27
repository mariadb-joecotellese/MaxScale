add_library(wcar SHARED
  wcar.cc
  wcarconfig.cc
  wcarfilter.cc
  wcarfiltersession.cc
  wcarrecorder.cc
  wcarinmemorystorage.cc
  )
target_link_libraries(wcar maxscale-common)
set_target_properties(wcar PROPERTIES VERSION "1.0.0" LINK_FLAGS -Wl,-z,defs)
install_module(wcar core)
