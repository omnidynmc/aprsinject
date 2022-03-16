/**************************************************************************
 ** Dynamic Networking Solutions                                         **
 **************************************************************************
 ** HAL9000, Internet Relay Chat Bot                                     **
 ** Copyright (C) 1999 Gregory A. Carter                                 **
 **                    Daniel Robert Karrels                             **
 **                    Dynamic Networking Solutions                      **
 **                                                                      **
 ** This program is free software; you can redistribute it and/or modify **
 ** it under the terms of the GNU General Public License as published by **
 ** the Free Software Foundation; either version 1, or (at your option)  **
 ** any later version.                                                   **
 **                                                                      **
 ** This program is distributed in the hope that it will be useful,      **
 ** but WITHOUT ANY WARRANTY; without even the implied warranty of       **
 ** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        **
 ** GNU General Public License for more details.                         **
 **                                                                      **
 ** You should have received a copy of the GNU General Public License    **
 ** along with this program; if not, write to the Free Software          **
 ** Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.            **
 **************************************************************************
 $Id: APNS.cpp,v 1.12 2003/09/05 22:23:41 omni Exp $
 **************************************************************************/

#include <fstream>
#include <string>
#include <queue>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <list>
#include <map>
#include <new>
#include <iostream>
#include <fstream>

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include <netdb.h>
#include <unistd.h>
#include <math.h>
#include <signal.h>

#include <openframe/openframe.h>

#include "MemcachedController.h"

namespace aprsinject {

/**************************************************************************
 ** MemcachedController Class                                            **
 **************************************************************************/

  MemcachedController::MemcachedController(const std::string &memcachedServers) : _memcachedServers(memcachedServers) {
    memcached_return rc;

    _expire = 0;

    if (!_memcachedServers.length())
      throw MemcachedController_Exception("invalid memcached server list");

    _st = memcached_create(NULL);

    if (_st == NULL)
      throw MemcachedController_Exception("unable to create memcached instance");

    _servers = memcached_servers_parse(_memcachedServers.c_str());
    if (_servers == NULL)
      throw MemcachedController_Exception("unable to parse memcached servers list");

    rc = memcached_server_push(_st, _servers);
    if (rc != MEMCACHED_SUCCESS)
      throw MemcachedController_Exception("unable to push memcached server list; "
        + std::string(memcached_strerror(_st, rc)));

  } // MemcachedController::MemcachedController

  MemcachedController::~MemcachedController() {
    memcached_server_list_free(_servers);
    memcached_free(_st);
  } // MemcachedController::~MemcachedController

  void MemcachedController::flush(const time_t expire) {
    memcached_flush(_st, expire);
  } // MemcachedController::flush

  void MemcachedController::put(const std::string &ns, const std::string &key, const std::string &value) {
    put(ns, key, value, _expire);
  } // MemcachedController::put

  void MemcachedController::put(const std::string &ns, const std::string &key, const std::string &value, const time_t expires) {
    std::string cacheKey = ns + ":" + key;
    memcached_return rc;
    uint32_t optflags = 0;

    assert(_st != NULL);		// bug

    if (cacheKey.length() < 1)
      throw MemcachedController_Exception("memcached namespace and key must not be 0 length");

    if (cacheKey.length() > 255)
      throw MemcachedController_Exception("memcached namespace and key must be less than 256 characters");

    rc = memcached_set(_st, cacheKey.c_str(), cacheKey.length(), value.data(), value.size(),
                       expires, optflags);

    if (rc != MEMCACHED_SUCCESS) {
      throw MemcachedController_Exception("memcached unable to set; "
        + std::string(memcached_strerror(_st, rc)));
    } // if

  } // MemcachedController::put

  void MemcachedController::replace(const std::string &ns, const std::string &key, const std::string &value) {
    replace(ns, key, value, _expire);
  } // MemcachedController::replace

  void MemcachedController::replace(const std::string &ns, const std::string &key, const std::string &value, const time_t expires) {
    std::string cacheKey = ns + ":" + key;
    memcached_return rc;
    uint32_t optflags = 0;

    assert(_st != NULL);		// bug

    if (cacheKey.length() < 1)
      throw MemcachedController_Exception("memcached namespace and key must not be 0 length");

    if (cacheKey.length() > 255)
      throw MemcachedController_Exception("memcached namespace and key must be less than 256 characters");

    rc = memcached_replace(_st, cacheKey.c_str(), cacheKey.length(), value.data(), value.size(),
                       expires, optflags);

    if (rc != MEMCACHED_SUCCESS) {
      throw MemcachedController_Exception("memcached unable to replace; "
        + std::string(memcached_strerror(_st, rc)));
    } // if

  } // MemcachedController::replace

  const MemcachedController::memcachedReturnEnum MemcachedController::get(const std::string &ns, const std::string &key, std::string &buf) {
    std::string cacheKey = ns + ":" + key;
    memcachedReturnEnum ret;
    char *str;
    memcached_return rc;
    uint32_t opt_flags = 0;
    size_t str_length;

    assert(_st != NULL);		// bug

    if (cacheKey.length() < 1)
      throw MemcachedController_Exception("memcached namespace and key must not be 0 length");

    if (cacheKey.length() > 255)
      throw MemcachedController_Exception("memcached namespace and key must be less than 256 characters");

    str = memcached_get(_st, cacheKey.c_str (), cacheKey.length(), &str_length, &opt_flags, &rc);

    switch(rc) {
      case MEMCACHED_SUCCESS:
        buf = std::string(str, str_length);
        ret = MEMCACHED_CONTROLLER_SUCCESS;
        break;
      case MEMCACHED_NOTFOUND:
        ret = MEMCACHED_CONTROLLER_NOTFOUND;
        break;
      default:
        ret = MEMCACHED_CONTROLLER_ERROR;
        break;
    } // switch

    if (str)
      free(str);

    if (ret == MEMCACHED_CONTROLLER_ERROR)
      throw MemcachedController_Exception("memcached unable to get; "
            + std::string(memcached_strerror(_st, rc)));

    return ret;
  } // MemcachedController::get

} // namespace openaprs
