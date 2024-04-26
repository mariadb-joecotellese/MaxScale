add_executable(maxplayer
repplayermain.cc
repplayer.cc
repconfig.cc
repsession.cc
repshow.cc
reptransform.cc
reprecorder.cc
repbooststorage.cc
repcsvstorage.cc)
target_link_libraries(maxplayer wcar-core maxscale-common boost_serialization)
if (HAVE_STD_EXECUTION)
  target_link_libraries(maxplayer tbb)
endif()
install_executable(maxplayer enterprise)
