add_executable(player
wcarplayermain.cc
wcarplayer.cc
wcarplayerconfig.cc
wcarplayersession.cc
wcartransform.cc)
target_link_libraries(player wcar-core maxbase boost_serialization)
