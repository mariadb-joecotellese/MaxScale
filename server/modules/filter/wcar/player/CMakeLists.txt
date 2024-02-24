add_executable(player
repplayermain.cc
repplayer.cc
repconfig.cc
repsession.cc
reptransform.cc
reprecorder.cc)
target_link_libraries(player wcar-core maxbase boost_serialization tbb)
