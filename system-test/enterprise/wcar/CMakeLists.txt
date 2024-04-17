# WCAR tests
add_test_executable_ex(NAME wcar_basic SOURCE wcar_basic.cc CONFIG wcar.cnf VMS repl_backend LABELS ENTERPRISE)
