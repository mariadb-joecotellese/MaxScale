add_executable(test_wcar_repstorage test_wcar_repstorage.cc ../repcsvstorage.cc ../repbooststorage.cc)
target_link_libraries(test_wcar_repstorage maxbase boost_serialization)
add_test(test_wcar_repstorage test_wcar_repstorage)
