add_executable(maxplayer
repplayermain.cc
repplayer.cc
repconfig.cc
repsession.cc
reptransform.cc
reprecorder.cc
repbooststorage.cc
repcsvstorage.cc)
target_link_libraries(maxplayer wcar-core maxscale-common boost_serialization tbb)
install_executable(maxplayer enterprise)
