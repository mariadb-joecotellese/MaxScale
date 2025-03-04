# Set the install layout
include(GNUInstallDirs)

set(MAXSCALE_LIBDIR ${CMAKE_INSTALL_LIBDIR}/maxscale CACHE PATH "Library installation path")
set(MAXSCALE_BINDIR ${CMAKE_INSTALL_BINDIR} CACHE PATH "Executable installation path")
set(MAXSCALE_SHAREDIR ${CMAKE_INSTALL_DATADIR}/maxscale CACHE PATH "Share file installation path, includes licence and readme files")
set(MAXSCALE_DOCDIR ${CMAKE_INSTALL_DOCDIR}/maxscale CACHE PATH "Documentation installation path, text versions only")

# These are the only hard-coded absolute paths
set(MAXSCALE_VARDIR /var CACHE PATH "Data file path (usually /var/)")
set(MAXSCALE_CONFDIR /etc CACHE PATH "Configuration file installation path (/etc/)")

# Default values for directories and subpaths where files are searched. These
# are used in `include/maxscale/paths.hh.in`.
set(DEFAULT_PID_SUBPATH "run/maxscale" CACHE PATH "Default PID file subpath")
set(DEFAULT_LOG_SUBPATH "log/maxscale" CACHE PATH "Default log subpath")
set(DEFAULT_DATA_SUBPATH "lib/maxscale" CACHE PATH "Default datadir subpath")
set(DEFAULT_LIB_SUBPATH "${MAXSCALE_LIBDIR}" CACHE PATH "Default library subpath")
set(DEFAULT_SHARE_SUBPATH "${MAXSCALE_SHAREDIR}" CACHE PATH "Default share subpath")
set(DEFAULT_CACHE_SUBPATH "cache/maxscale" CACHE PATH "Default cache subpath")
set(DEFAULT_LANG_SUBPATH "lib/maxscale" CACHE PATH "Default language file subpath")
set(DEFAULT_EXEC_SUBPATH "${MAXSCALE_BINDIR}" CACHE PATH "Default executable subpath")
set(DEFAULT_CONFIG_SUBPATH "etc" CACHE PATH "Default configuration subpath")
set(DEFAULT_CONFIG_PERSIST_SUBPATH "maxscale.cnf.d" CACHE PATH "Default persisted configuration subpath")
set(DEFAULT_MODULE_CONFIG_SUBPATH "${DEFAULT_CONFIG_SUBPATH}/maxscale.modules.d" CACHE PATH "Default configuration subpath")
set(DEFAULT_CONNECTOR_PLUGIN_SUBPATH "${MAXSCALE_LIBDIR}/plugin" CACHE PATH "Default connector plugin subpath")

set(DEFAULT_PIDDIR ${MAXSCALE_VARDIR}/${DEFAULT_PID_SUBPATH} CACHE PATH "Default PID file directory")
set(DEFAULT_LOGDIR ${MAXSCALE_VARDIR}/${DEFAULT_LOG_SUBPATH} CACHE PATH "Default log directory")
set(DEFAULT_DATADIR ${MAXSCALE_VARDIR}/${DEFAULT_DATA_SUBPATH} CACHE PATH "Default datadir path")
set(DEFAULT_LIBDIR ${CMAKE_INSTALL_PREFIX}/${DEFAULT_LIB_SUBPATH}/ CACHE PATH "Default library path")
set(DEFAULT_SHAREDIR ${CMAKE_INSTALL_PREFIX}/${DEFAULT_SHARE_SUBPATH}/ CACHE PATH "Default share path")
set(DEFAULT_CACHEDIR ${MAXSCALE_VARDIR}/${DEFAULT_CACHE_SUBPATH} CACHE PATH "Default cache directory")
set(DEFAULT_LANGDIR ${MAXSCALE_VARDIR}/${DEFAULT_LANG_SUBPATH} CACHE PATH "Default language file directory")
set(DEFAULT_EXECDIR ${CMAKE_INSTALL_PREFIX}/${DEFAULT_EXEC_SUBPATH} CACHE PATH "Default executable directory")
set(DEFAULT_CONFIGDIR /${DEFAULT_CONFIG_SUBPATH} CACHE PATH "Default configuration directory")
set(DEFAULT_CONFIGSUBDIR ${DEFAULT_CONFIGDIR}/maxscale.cnf.d CACHE PATH "Default configuration subdirectory")
set(DEFAULT_CONFIG_PERSISTDIR ${DEFAULT_DATADIR}/${DEFAULT_CONFIG_PERSIST_SUBPATH} CACHE PATH "Default persisted configuration directory")
set(DEFAULT_MODULE_CONFIGDIR /${DEFAULT_MODULE_CONFIG_SUBPATH} CACHE PATH "Default module configuration directory")
set(DEFAULT_CONNECTOR_PLUGINDIR ${CMAKE_INSTALL_PREFIX}/${DEFAULT_CONNECTOR_PLUGIN_SUBPATH} CACHE PATH "Default connector plugin directory")
set(DEFAULT_SYSTEMD_CONFIGDIR "/etc/systemd/system/maxscale.service.d" CACHE PATH "Default SystemD configuration drop-in directory")

# Install the empty directores as a part of the core maxscale package
install(DIRECTORY DESTINATION ${MAXSCALE_DOCDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${MAXSCALE_SHAREDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_CACHEDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_PIDDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_LOGDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_DATADIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_MODULE_CONFIGDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_CONFIGSUBDIR} COMPONENT core)
install(DIRECTORY DESTINATION ${DEFAULT_SYSTEMD_CONFIGDIR} COMPONENT core)

# Name of the common core library
set(MAXSCALE_CORE maxscale-common)

#
# Installation functions for MaxScale
#
# Do not directly install files with install(...) etc. commands. Use these
# functions for all executables, modules and files that are to be installed
# as a part of a package. These functions make additional checks that all
# required parameters are present for the modules and make sure that the
# targets are installed into right directories where MaxScale can find them.
#

# Installation functions for executables and modules.
#
# @param Name of the CMake target
# @param Component where this executable should be included
function(install_executable target component)
  install(TARGETS ${target} DESTINATION ${MAXSCALE_BINDIR} COMPONENT "${component}")
endfunction()

function(install_executable_setuid target component)
  install(TARGETS ${target} DESTINATION ${MAXSCALE_BINDIR} COMPONENT "${component}" PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE SETUID GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
endfunction()

# Installation function for modules
#
# @param Name of the CMake target
# @param Component where this module should be included
function(install_module target component)
  get_target_property(TGT_VERSION ${target} VERSION)

  if (${TGT_VERSION} MATCHES "NOTFOUND")
    message(AUTHOR_WARNING "Module '${target}' is missing the VERSION parameter!")
  endif()

  install(TARGETS ${target} DESTINATION ${MAXSCALE_LIBDIR} COMPONENT "${component}")

  # Make all modules dependent on the core
  add_dependencies(${target} ${MAXSCALE_CORE})
endfunction()

# Installation functions for interpreted scripts.
#
# @param Script to install
# @param Component where this script should be included
function(install_script target component)
  install(PROGRAMS ${target} DESTINATION ${MAXSCALE_BINDIR}
    PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ
    COMPONENT "${component}")
endfunction()

# Installation functions for files and programs. These all go to the share directory
# of MaxScale which for packages is /usr/share/maxscale.
#
# @param File to install
# @param Component where this file should be included
function(install_file file component)
  install(FILES ${file} DESTINATION ${MAXSCALE_SHAREDIR} COMPONENT "${component}")
endfunction()

function(install_program file component)
  install(PROGRAMS ${file} DESTINATION ${MAXSCALE_SHAREDIR} COMPONENT "${component}")
endfunction()

# Install man pages
#
# @param Manual file to install
# @param The page number where this should be installed e.g. man1
# @param Component where this manual should be included
function(install_manual file page component)
  install(PROGRAMS ${file} DESTINATION ${CMAKE_INSTALL_DATADIR}/man/man${page} COMPONENT "${component}")
endfunction()

# Install headers
#
# @param Header to install
# @param Component where this header should be included
function(install_header header component)
  install(FILES ${header} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/maxscale COMPONENT "${component}")
endfunction()

# Install development library
#
# @param Target to install
# @param Component where this library should be included
function(install_dev_library lib component)
  install(TARGETS ${lib} DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT "${component}")
endfunction()


# Install custom file to a custom destination
#
# @param File to install
# @param Destination where to install the file
# @param Component where this file should be included
function(install_custom_file file dest component)
  install(FILES ${file} DESTINATION ${dest} COMPONENT "${component}")
endfunction()

# Install custom script to a custom destination
#
# @param Script to install
# @param Destination where to install the script
# @param Component where this script should be included
function(install_custom_script script dest component)
  install(PROGRAMS ${script} DESTINATION ${dest}
    PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_READ GROUP_READ WORLD_READ
    COMPONENT "${component}")
endfunction()

# Install a directory with files
#
# @param Directory to install
# @param Destination where to install the directory
# @param Component where this file should be included
function(install_directory dir dest component)
  install(DIRECTORY ${dir} DESTINATION ${dest} COMPONENT "${component}")
endfunction()
