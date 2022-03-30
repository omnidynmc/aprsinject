#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([aprsinject], [1.0.0.1], [gcarter@openaprs.net])
AC_CONFIG_SRCDIR([src/main.cpp])
AC_CONFIG_HEADERS([include/config.h])
AM_INIT_AUTOMAKE
LT_INIT
AC_CONFIG_MACRO_DIR([m4])

AM_MAINTAINER_MODE

# Checks for programs.
AC_PROG_CXX
AC_PROG_LIBTOOL

AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_LN_S
AC_LANG([C++])

prefix="`pwd`"
bindir="`pwd`"
libdir="`pwd`/lib"
includedir="`pwd`/include"

ofincludedir="/usr/local/include/openframe"
oflibdir="/usr/local/lib"

osincludedir="/usr/local/include/openstats"
oslibdir="/usr/local/lib"

stincludedir="/usr/local/include/stomp"
stlibdir="/usr/local/lib"

aincludedir="/usr/local/include/aprs"
alibdir="/usr/local/lib"

mysql_include="/usr/include/mysql"
mysqlpp_include="/usr/include/mysql++"

CPPFLAGS="$CPPFLAGS -I/usr/include"
CXXFLAGS="-Wall -pipe -g -I$includedir -I$ofincludedir -I$stincludedir -I$osincludedir -I$aincludedir -I$mysql_include -I$mysqlpp_include"
CXX="g++"
LIBS="$LIBS"

# Checks for libraries.

AC_CHECK_LIB([dl], [dlopen], [], [
               echo "dynamic loadable library is required for this program"
               exit 1
             ])

AC_CHECK_LIB([pthread], [pthread_create], [], [
               echo "pthread library is required for this program"
               exit 1
             ])

AC_CHECK_LIB([readline], [readline], [], [
               echo "readline library is required for this program"
               exit 1
             ])

AC_CHECK_LIB([z], [gzread], [], [
               echo "zlib library is required for this program"
               exit 1
             ])

AC_CHECK_LIB([ossp-uuid++], [uuid_version], [], [
               echo "ossp-uuid++ library is required for this program"
               exit 1
             ])


dnl #########################
dnl ## Check for memcached ##
dnl #########################

echo "#########################"
echo "## libmemcached Checks ##"
echo "#########################"

PKG_CHECK_MODULES(DEPS, libmemcached >= 0.8.0) AC_SUBST(DEPS_CFLAGS) AC_SUBST(DEPS_LIBS)


# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_SUBST(bindir)
AC_SUBST(LIBTOOL_DEPS)

dnl CXXFLAGS="$CXXFLAGS -I`pwd`/include $DEPS_CFLAGS"
LIBS="$LIBS -L`pwd`/lib $DEPS_LIBS -L$oflibdir -L$stlibdir -L$oslibdir -L$alibdir -lopenframe -lstomp -lopenstats -laprs"

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 modules/Makefile
                 test/Makefile
               ])
AC_OUTPUT
