# WCAR tests
add_test_executable_ex(NAME wcar_basic SOURCE wcar_basic.cc CONFIG wcar.cnf VMS repl_backend LABELS ENTERPRISE)
add_test_executable_ex(NAME wcar_live_capture SOURCE wcar_live_capture.cc CONFIG wcar_live_capture.cnf VMS repl_backend LABELS ENTERPRISE)
add_test_executable_ex(NAME wcar_deadlocks SOURCE wcar_deadlocks.cc CONFIG wcar.cnf VMS repl_backend LABELS ENTERPRISE)
add_test_executable_ex(NAME wcar_short_connections SOURCE wcar_short_connections.cc CONFIG wcar_live_capture.cnf VMS repl_backend LABELS ENTERPRISE)
