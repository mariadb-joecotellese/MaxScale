configure_file(maxvisualize.in ${CMAKE_CURRENT_BINARY_DIR}/maxvisualize @ONLY)
configure_file(maxpostprocess.in ${CMAKE_CURRENT_BINARY_DIR}/maxpostprocess @ONLY)
configure_file(prepare_venv.sh.in ${CMAKE_CURRENT_BINARY_DIR}/prepare_venv.sh @ONLY)

install_script(${CMAKE_CURRENT_BINARY_DIR}/maxvisualize enterprise)
install_script(${CMAKE_CURRENT_BINARY_DIR}/maxpostprocess enterprise)

install_custom_script(maxpostprocess.py ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_script(${CMAKE_CURRENT_BINARY_DIR}/prepare_venv.sh ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_file(requirements.txt ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_file(requirements-notebook.txt ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_file(notebook_helper.py ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_file(WCAR.ipynb ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
install_custom_file(README.md ${MAXSCALE_SHAREDIR}/wcar/ enterprise)
