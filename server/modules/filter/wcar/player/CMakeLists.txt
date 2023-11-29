add_executable(player wcarplayer.cc wcarplayerconfig.cc)
target_link_libraries(player wcar-core maxbase)
