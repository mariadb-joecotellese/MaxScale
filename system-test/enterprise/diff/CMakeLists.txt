# DIFF tests
add_test_executable_ex(NAME diff_test_runtime SOURCE diff_test_runtime.cc CONFIG diff_test_runtime.cnf VMS repl_backend LABELS ENTERPRISE)
add_test_executable_ex(NAME diff_test_static SOURCE diff_test_static.cc CONFIG diff_test_static.cnf VMS repl_backend LABELS ENTERPRISE)
