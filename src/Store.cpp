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

#include <string>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <new>
#include <iostream>

#include <errno.h>
#include <time.h>
#include <math.h>

#include <aprs/APRS.h>

#include <openframe/openframe.h>
#include <openstats/StatsClient_Interface.h>

#include "DBI.h"
#include "MemcachedController.h"
#include "Store.h"

namespace aprsinject {
  using namespace openframe::loglevel;

/**************************************************************************
 ** Store Class                                                         **
 **************************************************************************/
  const time_t Store::kDefaultReportInterval			= 3600;

  Store::Store(const openframe::LogObject::thread_id_t thread_id,
               const std::string &host,
               const std::string &user,
               const std::string &pass,
               const std::string &db,
               const std::string &memcached_host,
               const time_t expire_interval,
               const time_t report_interval)
        : openframe::LogObject(thread_id),
          _host(host),
          _user(user),
          _pass(pass),
          _db(db),
          _memcached_host(memcached_host),
          _expire_interval(expire_interval) {


    init_stats(_stats, true);
    init_stats(_stompstats, true);

    _stats.report_interval = report_interval;
    _stompstats.report_interval = 5;

    _last_cache_fail_at = 0;

    _dbi = NULL;
    _memcached = NULL;
    _profile = NULL;
  } // Store::Store

  Store::~Store() {
    if (_memcached) delete _memcached;
    if (_dbi) delete _dbi;
    if (_profile) delete _profile;
  } // Store::~Store

  Store &Store::init() {
    try {
      _dbi = new DBI(thread_id(), _host, _user, _pass, _db);
      _dbi->set_elogger( elogger(), elog_name() );
      _dbi->init();
    } // try
    catch(std::bad_alloc &xa) {
      assert(false);
    } // catch

    _memcached = new MemcachedController(_memcached_host);
    _memcached->expire(_expire_interval);

    _profile = new openframe::Stopwatch();
    _profile->add("memcached.callsign", 300);
    _profile->add("memcached.icon", 300);
    _profile->add("memcached.name", 300);
    _profile->add("memcached.path", 300);
    _profile->add("memcached.status", 300);
    _profile->add("memcached.duplicates", 300);
    _profile->add("memcached.position", 300);
    _profile->add("memcached.locatorseen", 300);
    _profile->add("memcached.positions", 300);
    _profile->add("sql.insert.path", 300);

    return *this;
  } // Store::init

  void Store::init_stats(obj_stats_t &stats, const bool startup) {
    memset(&stats.cache_store, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_callsign, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_icon, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_name, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_dest, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_digi, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_message, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_packet, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_path, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_status, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_duplicates, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_positions, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_position, 0, sizeof(memcache_stats_t) );
    memset(&stats.cache_locatorseen, 0, sizeof(memcache_stats_t) );

    memset(&stats.sql_store, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_callsign, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_icon, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_digi, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_message, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_dest, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_name, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_packet, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_path, 0, sizeof(sql_stats_t) );
    memset(&stats.sql_status, 0, sizeof(sql_stats_t) );

    memset(&stats.prof_cache_locatorseen, 0, sizeof(profile_stats_t) );
    memset(&stats.prof_cache_lastpositions, 0, sizeof(profile_stats_t) );
    memset(&stats.prof_cache_positions, 0, sizeof(profile_stats_t) );
    memset(&stats.prof_sql_message, 0, sizeof(profile_stats_t) );
    memset(&stats.prof_sql_path, 0, sizeof(profile_stats_t) );

    stats.last_report_at = time(NULL);
    if (startup) stats.created_at = time(NULL);
  } // init_stats

  void Store::onDescribeStats() {
    describe_root_stat("store.num.cache.store.hits", "store/cache/store/num hits - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.store.misses", "store/cache/store/num misses - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.store.tries", "store/cache/store/num tries - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.store.stored", "store/cache/store/num stored - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.store.hitrate", "store/cache/store/num hitrate - store", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.callsign.hits", "store/cache/callsign/num hits - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.callsign.misses", "store/cache/callsign/num misses - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.callsign.tries", "store/cache/callsign/num tries - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.callsign.stored", "store/cache/callsign/num stored - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.callsign.hitrate", "store/cache/callsign/num hitrate - callsign", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.icon.hits", "store/cache/icon/num hits - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.icon.misses", "store/cache/icon/num misses - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.icon.tries", "store/cache/icon/num tries - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.icon.stored", "store/cache/icon/num stored - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.icon.hitrate", "store/cache/icon/num hitrate - icon", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.name.hits", "store/cache/object name/num hits - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.name.misses", "store/cache/object name/num misses - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.name.tries", "store/cache/object name/num tries - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.name.stored", "store/cache/object name/num stored - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.name.hitrate", "store/cache/object name/num hitrate - object name", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.dest.hits", "store/cache/dest/num hits - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.dest.misses", "store/cache/dest/num misses - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.dest.tries", "store/cache/dest/num tries - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.dest.stored", "store/cache/dest/num stored - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.dest.hitrate", "store/cache/dest/num hitrate - dest", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.digi.hits", "store/cache/digi/num hits - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.digi.misses", "store/cache/digi/num misses - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.digi.tries", "store/cache/digi/num tries - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.digi.stored", "store/cache/digi/num stored - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.digi.hitrate", "store/cache/digi/num hitrate - digi", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.message.hits", "store/cache/message/num hits - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.message.misses", "store/cache/message/num misses - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.message.tries", "store/cache/message/num tries - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.message.stored", "store/cache/message/num stored - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.message.hitrate", "store/cache/message/num hitrate - message", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.packet.hits", "store/cache/packet/num hits - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.packet.misses", "store/cache/packet/num misses - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.packet.tries", "store/cache/packet/num tries - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.packet.stored", "store/cache/packet/num stored - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.packet.hitrate", "store/cache/packet/num hitrate - packet", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.path.hits", "store/cache/path/num hits - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.path.misses", "store/cache/path/num misses - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.path.tries", "store/cache/path/num tries - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.path.stored", "store/cache/path/num stored - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.path.hitrate", "store/cache/path/num hitrate - path", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.status.hits", "store/cache/status/num hits - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.status.misses", "store/cache/status/num misses - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.status.tries", "store/cache/status/num tries - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.status.stored", "store/cache/status/num stored - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.status.hitrate", "store/cache/status/num hitrate - status", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.duplicates.hits", "store/cache/duplicates/num hits - duplicates", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.duplicates.misses", "store/cache/duplicates/num misses - duplicates", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.duplicates.tries", "store/cache/duplicates/num tries - duplicates", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.duplicates.stored", "store/cache/duplicates/num stored - duplicates", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.duplicates.hitrate", "store/cache/duplicates/num hitrate - duplicates", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.locator.stored", "store/cache/locator/num stored - locator seen", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.locator.time.put", "store/cache/locator/time - put locator seen microseconds", openstats::graphTypeGauge, openstats::dataTypeInt);

    describe_root_stat("store.num.cache.positions.hits", "store/cache/positions/num hits - positions", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.positions.misses", "store/cache/positions/num misses - positions", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.positions.tries", "store/cache/positions/num tries - positions", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.positions.stored", "store/cache/positions/num stored - positions", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.positions.hitrate", "store/cache/positions/num hitrate - positions", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.cache.position.hits", "store/cache/position/num hits - position", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.position.misses", "store/cache/position/num misses - position", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.position.tries", "store/cache/position/num tries - position", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.position.stored", "store/cache/position/num stored - position", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.cache.position.hitrate", "store/cache/position/num hitrate - position", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_root_stat("store.num.sql.store.hits", "store/sql/store/num hits - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.store.misses", "store/sql/store/num misses - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.store.tries", "store/sql/store/num tries - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.store.inserted", "store/sql/store/num inserted - store", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.store.failed", "store/sql/store/num failed - store", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.callsign.hits", "store/sql/callsign/num hits - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.callsign.misses", "store/sql/callsign/num misses - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.callsign.tries", "store/sql/callsign/num tries - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.callsign.hitrate", "store/sql/callsign/num hitrate - callsign", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.callsign.inserted", "store/sql/callsign/num inserted - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.callsign.failed", "store/sql/callsign/num failed - callsign", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.icon.hits", "store/sql/icon/num hits - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.icon.misses", "store/sql/icon/num misses - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.icon.tries", "store/sql/icon/num tries - icon", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.icon.hitrate", "store/sql/icon/num hitrate - icon", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.icon.failed", "store/sql/icon/num failed - icon", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.name.hits", "store/sql/object name/num hits - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.name.misses", "store/sql/object name/num misses - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.name.tries", "store/sql/object name/num tries - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.name.hitrate", "store/sql/object name/num hitrate - object name", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.name.inserted", "store/sql/object name/num inserted - object name", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.name.failed", "store/sql/object name/num failed - object name", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.dest.hits", "store/sql/dest/num hits - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.dest.misses", "store/sql/dest/num misses - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.dest.tries", "store/sql/dest/num tries - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.dest.hitrate", "store/sql/dest/num hitrate - dest", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.dest.inserted", "store/sql/dest/num inserted - dest", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.dest.failed", "store/sql/dest/num failed - dest", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.digi.hits", "store/sql/digi/num hits - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.digi.misses", "store/sql/digi/num misses - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.digi.tries", "store/sql/digi/num tries - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.digi.hitrate", "store/sql/digi/num hitrate - digi", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.digi.inserted", "store/sql/digi/num inserted - digi", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.digi.failed", "store/sql/digi/num failed - digi", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.message.hits", "store/sql/message/num hits - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.message.misses", "store/sql/message/num misses - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.message.tries", "store/sql/message/num tries - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.message.hitrate", "store/sql/message/num hitrate - message", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.message.inserted", "store/sql/message/num inserted - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.message.failed", "store/sql/message/num failed - message", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.message.time.get", "store/sql/message/time - get message microseconds", openstats::graphTypeGauge, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.packet.hits", "store/sql/packet/num hits - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.packet.misses", "store/sql/packet/num missess - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.packet.tries", "store/sql/packet/num tries - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.packet.inserted", "store/sql/packet/num inserted - packet", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.packet.failed", "store/sql/packet/num failed - packet", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.path.hits", "store/sql/path/num hits - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.path.misses", "store/sql/path/num misses - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.path.tries", "store/sql/path/num tries - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.path.hitrate", "store/sql/path/num hitrate - path", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.path.inserted", "store/sql/path/num inserted - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.path.failed", "store/sql/path/num failed - path", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.path.time.get", "store/sql/path/time - get path microseconds", openstats::graphTypeGauge, openstats::dataTypeInt);

    describe_root_stat("store.num.sql.status.hits", "store/sql/status/num hits - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.status.misses", "store/sql/status/num misses - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.status.tries", "store/sql/status/num tries - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.status.hitrate", "store/sql/status/num hitrate - status", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_root_stat("store.num.sql.status.inserted", "store/sql/status/num inserted - status", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_root_stat("store.num.sql.status.failed", "store/sql/status/num failed - status", openstats::graphTypeCounter, openstats::dataTypeInt);
  } // Store::onDescribeStats

  void Store::onDestroyStats() {
    destroy_stat("store.num.*");
  } // Store::onDestroyStats

  void Store::try_stats() {
    try_stompstats();

    if (_stats.last_report_at > time(NULL) - _stats.report_interval) return;

    TLOG(LogNotice, << "Memcached{callsign} hits "
                    << _stats.cache_callsign.hits
                    << ", misses "
                    << _stats.cache_callsign.misses
                    << ", tries "
                    << _stats.cache_callsign.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.cache_callsign.hits, _stats.cache_callsign.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.callsign")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{name} hits "
                    << _stats.cache_name.hits
                    << ", misses "
                    << _stats.cache_name.misses
                    << ", tries "
                    << _stats.cache_name.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_name.hits, _stats.cache_name.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.name")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{icon} hits "
                    << _stats.cache_icon.hits
                    << ", misses "
                    << _stats.cache_icon.misses
                    << ", tries "
                    << _stats.cache_icon.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.cache_icon.hits, _stats.cache_icon.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.icon")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{dest} hits "
                    << _stats.cache_dest.hits
                    << ", misses "
                    << _stats.cache_dest.misses
                    << ", tries "
                    << _stats.cache_dest.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_dest.hits, _stats.cache_dest.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.dest")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{digi} hits "
                    << _stats.cache_digi.hits
                    << ", misses "
                    << _stats.cache_digi.misses
                    << ", tries "
                    << _stats.cache_digi.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_digi.hits, _stats.cache_digi.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.digi")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{message} hits "
                    << _stats.cache_message.hits
                    << ", misses "
                    << _stats.cache_message.misses
                    << ", tries "
                    << _stats.cache_message.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_message.hits, _stats.cache_message.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.message")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{path} hits "
                    << _stats.cache_path.hits
                    << ", misses "
                    << _stats.cache_path.misses
                    << ", tries "
                    << _stats.cache_path.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_path.hits, _stats.cache_path.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.path")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{status} hits "
                    << _stats.cache_status.hits
                    << ", misses "
                    << _stats.cache_status.misses
                    << ", tries "
                    << _stats.cache_status.tries
                    << ", rate %"
                    << OPENSTATS_PERCENT(_stats.cache_status.hits, _stats.cache_status.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.status")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{pos} hits "
                    << _stats.cache_position.hits
                    << ", misses "
                    << _stats.cache_position.misses
                    << ", tries "
                    << _stats.cache_position.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.cache_position.hits, _stats.cache_position.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.position")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{poss} hits "
                    << _stats.cache_positions.hits
                    << ", misses "
                    << _stats.cache_positions.misses
                    << ", tries "
                    << _stats.cache_positions.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.cache_positions.hits, _stats.cache_positions.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.positions")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Memcached{dups} hits "
                    << _stats.cache_duplicates.hits
                    << ", misses "
                    << _stats.cache_duplicates.misses
                    << ", tries "
                    << _stats.cache_duplicates.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.cache_duplicates.hits, _stats.cache_duplicates.tries)
                    << " average "
                    << std::fixed << std::setprecision(4)
                    << _profile->average("memcached.duplicates")
                    << "s"
                    << std::endl);

    TLOG(LogNotice, << "Sql{callsign} hits "
                    << _stats.sql_callsign.hits
                    << ", misses "
                    << _stats.sql_callsign.misses
                    << ", tries "
                    << _stats.sql_callsign.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_callsign.hits, _stats.sql_callsign.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{name} hits "
                    << _stats.sql_name.hits
                    << ", misses "
                    << _stats.sql_name.misses
                    << ", tries "
                    << _stats.sql_name.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_name.hits, _stats.sql_name.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{icon} hits "
                    << _stats.sql_icon.hits
                    << ", misses "
                    << _stats.sql_icon.misses
                    << ", tries "
                    << _stats.sql_icon.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_icon.hits, _stats.sql_icon.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{dest} hits "
                    << _stats.sql_dest.hits
                    << ", misses "
                    << _stats.sql_dest.misses
                    << ", tries "
                    << _stats.sql_dest.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_dest.hits, _stats.sql_dest.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{digi} hits "
                    << _stats.sql_digi.hits
                    << ", misses "
                    << _stats.sql_digi.misses
                    << ", tries "
                    << _stats.sql_digi.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_digi.hits, _stats.sql_digi.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{message} hits "
                    << _stats.sql_message.hits
                    << ", misses "
                    << _stats.sql_message.misses
                    << ", tries "
                    << _stats.sql_message.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_message.hits, _stats.sql_message.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{path} hits "
                    << _stats.sql_path.hits
                    << ", misses "
                    << _stats.sql_path.misses
                    << ", tries "
                    << _stats.sql_path.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_path.hits, _stats.sql_path.tries)
                    << std::endl);

    TLOG(LogNotice, << "Sql{status} hits "
                    << _stats.sql_status.hits
                    << ", misses "
                    << _stats.sql_status.misses
                    << ", tries "
                    << _stats.sql_status.tries
                    << ", rate %"
                    << std::fixed << std::setprecision(2)
                    << OPENSTATS_PERCENT(_stats.sql_status.hits, _stats.sql_status.tries)
                    << std::endl);

    init_stats(_stats);
  } // Store::try_stats

  void Store::try_stompstats() {
    if (_stompstats.last_report_at > time(NULL) - _stompstats.report_interval) return;

    // this prevents stompstats from having to lookup strings in
    // its hash tables over and over again in realtime at ~35 pps

    datapoint("store.num.sql.callsign.tries", _stompstats.sql_callsign.tries);
    datapoint("store.num.sql.callsign.hits", _stompstats.sql_callsign.hits);
    datapoint_float("store.num.sql.callsign.hitrate", OPENSTATS_PERCENT(_stompstats.sql_callsign.hits, _stompstats.sql_callsign.tries) );
    datapoint("store.num.sql.callsign.misses", _stompstats.sql_callsign.misses);
    datapoint("store.num.sql.callsign.inserted", _stompstats.sql_callsign.inserted);
    datapoint("store.num.sql.callsign.failed", _stompstats.sql_callsign.failed);

    datapoint("store.num.sql.icon.tries", _stompstats.sql_icon.tries);
    datapoint("store.num.sql.icon.hits", _stompstats.sql_icon.hits);
    datapoint_float("store.num.sql.icon.hitrate", OPENSTATS_PERCENT(_stompstats.sql_icon.hits, _stompstats.sql_icon.tries) );
    datapoint("store.num.sql.icon.misses", _stompstats.sql_icon.misses);
    datapoint("store.num.sql.icon.failed", _stompstats.sql_icon.failed);

    datapoint("store.num.sql.name.tries", _stompstats.sql_name.tries);
    datapoint("store.num.sql.name.hits", _stompstats.sql_name.hits);
    datapoint_float("store.num.sql.name.hitrate", OPENSTATS_PERCENT(_stompstats.sql_name.hits, _stompstats.sql_name.tries) );
    datapoint("store.num.sql.name.misses", _stompstats.sql_name.misses);
    datapoint("store.num.sql.name.inserted", _stompstats.sql_name.inserted);
    datapoint("store.num.sql.name.failed", _stompstats.sql_name.failed);

    datapoint("store.num.sql.dest.tries", _stompstats.sql_dest.tries);
    datapoint("store.num.sql.dest.hits", _stompstats.sql_dest.hits);
    datapoint_float("store.num.sql.dest.hitrate", OPENSTATS_PERCENT(_stompstats.sql_dest.hits, _stompstats.sql_dest.tries) );
    datapoint("store.num.sql.dest.misses", _stompstats.sql_dest.misses);
    datapoint("store.num.sql.dest.inserted", _stompstats.sql_dest.inserted);
    datapoint("store.num.sql.dest.failed", _stompstats.sql_dest.failed);

    datapoint("store.num.sql.digi.tries", _stompstats.sql_digi.tries);
    datapoint("store.num.sql.digi.hits", _stompstats.sql_digi.hits);
    datapoint_float("store.num.sql.digi.hitrate", OPENSTATS_PERCENT(_stompstats.sql_digi.hits, _stompstats.sql_digi.tries) );
    datapoint("store.num.sql.digi.misses", _stompstats.sql_digi.misses);
    datapoint("store.num.sql.digi.inserted", _stompstats.sql_digi.inserted);
    datapoint("store.num.sql.digi.failed", _stompstats.sql_digi.failed);

    datapoint("store.num.sql.message.tries", _stompstats.sql_message.tries);
    datapoint("store.num.sql.message.hits", _stompstats.sql_message.hits);
    datapoint_float("store.num.sql.message.hitrate", OPENSTATS_PERCENT(_stompstats.sql_message.hits, _stompstats.sql_message.tries) );
    datapoint("store.num.sql.message.misses", _stompstats.sql_message.misses);
    datapoint("store.num.sql.message.inserted", _stompstats.sql_message.inserted);
    datapoint("store.num.sql.message.failed", _stompstats.sql_message.failed);
    datapoint("store.num.sql.message.time.get", _stompstats.prof_sql_message.mean);

    datapoint("store.num.sql.packet.inserted", _stompstats.sql_packet.inserted);
    datapoint("store.num.sql.packet.failed", _stompstats.sql_packet.failed);

    datapoint("store.num.cache.callsign.tries", _stompstats.cache_callsign.tries);
    datapoint("store.num.cache.callsign.misses", _stompstats.cache_callsign.misses);
    datapoint("store.num.cache.callsign.hits", _stompstats.cache_callsign.hits);
    datapoint_float("store.num.cache.callsign.hitrate", OPENSTATS_PERCENT(_stompstats.cache_callsign.hits, _stompstats.cache_callsign.tries) );
    datapoint("store.num.cache.callsign.stored", _stompstats.cache_callsign.stored);

    datapoint("store.num.cache.icon.tries", _stompstats.cache_icon.tries);
    datapoint("store.num.cache.icon.misses", _stompstats.cache_icon.misses);
    datapoint("store.num.cache.icon.hits", _stompstats.cache_icon.hits);
    datapoint_float("store.num.cache.icon.hitrate", OPENSTATS_PERCENT(_stompstats.cache_icon.hits, _stompstats.cache_icon.tries) );
    datapoint("store.num.cache.icon.stored", _stompstats.cache_icon.stored);

    datapoint("store.num.cache.name.tries", _stompstats.cache_name.tries);
    datapoint("store.num.cache.name.misses", _stompstats.cache_name.misses);
    datapoint("store.num.cache.name.hits", _stompstats.cache_name.hits);
    datapoint_float("store.num.cache.name.hitrate", OPENSTATS_PERCENT(_stompstats.cache_name.hits, _stompstats.cache_name.tries) );
    datapoint("store.num.cache.name.stored", _stompstats.cache_name.stored);

    datapoint("store.num.cache.dest.tries", _stompstats.cache_dest.tries);
    datapoint("store.num.cache.dest.misses", _stompstats.cache_dest.misses);
    datapoint("store.num.cache.dest.hits", _stompstats.cache_dest.hits);
    datapoint_float("store.num.cache.dest.hitrate", OPENSTATS_PERCENT(_stompstats.cache_dest.hits, _stompstats.cache_dest.tries) );
    datapoint("store.num.cache.dest.stored", _stompstats.cache_dest.stored);

    datapoint("store.num.cache.digi.tries", _stompstats.cache_digi.tries);
    datapoint("store.num.cache.digi.misses", _stompstats.cache_digi.misses);
    datapoint("store.num.cache.digi.hits", _stompstats.cache_digi.hits);
    datapoint_float("store.num.cache.digi.hitrate", OPENSTATS_PERCENT(_stompstats.cache_digi.hits, _stompstats.cache_digi.tries) );
    datapoint("store.num.cache.digi.stored", _stompstats.cache_digi.stored);

    datapoint("store.num.cache.message.tries", _stompstats.cache_message.tries);
    datapoint("store.num.cache.message.misses", _stompstats.cache_message.misses);
    datapoint("store.num.cache.message.hits", _stompstats.cache_message.hits);
    datapoint_float("store.num.cache.message.hitrate", OPENSTATS_PERCENT(_stompstats.cache_message.hits, _stompstats.cache_message.tries) );
    datapoint("store.num.cache.message.stored", _stompstats.cache_message.stored);

    datapoint("store.num.sql.path.tries", _stompstats.sql_path.tries);
    datapoint("store.num.sql.path.hits", _stompstats.sql_path.hits);
    datapoint_float("store.num.sql.path.hitrate", OPENSTATS_PERCENT(_stompstats.sql_path.hits, _stompstats.sql_path.tries) );
    datapoint("store.num.sql.path.misses", _stompstats.sql_path.misses);
    datapoint("store.num.sql.path.inserted", _stompstats.sql_path.inserted);
    datapoint("store.num.sql.path.failed", _stompstats.sql_path.failed);
    datapoint("store.num.sql.path.time.get", _stompstats.prof_sql_path.mean);

    datapoint("store.num.cache.path.tries", _stompstats.cache_path.tries);
    datapoint("store.num.cache.path.misses", _stompstats.cache_path.misses);
    datapoint("store.num.cache.path.hits", _stompstats.cache_path.hits);
    datapoint_float("store.num.cache.path.hitrate", OPENSTATS_PERCENT(_stompstats.cache_path.hits, _stompstats.cache_path.tries) );
    datapoint("store.num.cache.path.stored", _stompstats.cache_path.stored);

    datapoint("store.num.sql.status.tries", _stompstats.sql_status.tries);
    datapoint("store.num.sql.status.hits", _stompstats.sql_status.hits);
    datapoint_float("store.num.sql.status.hitrate", OPENSTATS_PERCENT(_stompstats.sql_status.hits, _stompstats.sql_status.tries) );
    datapoint("store.num.sql.status.misses", _stompstats.sql_status.misses);
    datapoint("store.num.sql.status.inserted", _stompstats.sql_status.inserted);
    datapoint("store.num.sql.status.failed", _stompstats.sql_status.failed);

    datapoint("store.num.cache.status.tries", _stompstats.cache_status.tries);
    datapoint("store.num.cache.status.hits", _stompstats.cache_status.hits);
    datapoint("store.num.cache.status.misses", _stompstats.cache_status.misses);
    datapoint_float("store.num.cache.status.hitrate", OPENSTATS_PERCENT(_stompstats.cache_status.hits, _stompstats.cache_status.tries) );
    datapoint("store.num.cache.status.stored", _stompstats.cache_status.stored);

    datapoint("store.num.cache.duplicates.tries", _stompstats.cache_duplicates.tries);
    datapoint("store.num.cache.duplicates.misses", _stompstats.cache_duplicates.misses);
    datapoint("store.num.cache.duplicates.hits", _stompstats.cache_duplicates.hits);
    datapoint_float("store.num.cache.duplicates.hitrate", OPENSTATS_PERCENT(_stompstats.cache_duplicates.hits, _stompstats.cache_duplicates.tries) );
    datapoint("store.num.cache.duplicates.stored", _stompstats.cache_duplicates.stored);

    datapoint("store.num.cache.locator.stored", _stompstats.cache_locatorseen.stored);
    datapoint("store.num.cache.locator.time.put", _stompstats.prof_cache_locatorseen.mean);

    datapoint("store.num.cache.positions.tries", _stompstats.cache_positions.tries);
    datapoint("store.num.cache.positions.misses", _stompstats.cache_positions.misses);
    datapoint("store.num.cache.positions.hits", _stompstats.cache_positions.hits);
    datapoint_float("store.num.cache.positions.hitrate", OPENSTATS_PERCENT(_stompstats.cache_positions.hits, _stompstats.cache_positions.tries) );
    datapoint("store.num.cache.positions.stored", _stompstats.cache_positions.stored);

    init_stats(_stompstats);
  } // Store::try_stompstats()

  bool Store::getCallsignId(const std::string &source, std::string &ret_id) {
    int i;

    // try and find in memcached
    if (getCallsignIdFromMemcached(source, ret_id)) return true;

    // not in memcached find in sql
    _stats.sql_callsign.tries++;
    _stompstats.sql_callsign.tries++;
    if (_dbi->getCallsignId(source, ret_id)) {
      setCallsignIdInMemcached(source, ret_id);
      _stats.sql_callsign.hits++;
      _stompstats.sql_callsign.hits++;
      return true;
    } // if
    _stats.sql_callsign.misses++;
    _stompstats.sql_callsign.misses++;

    // try again just in case another thread beat us
    for(i=0; i < 3; i++) {
      // not in sql try and create it
      if (_dbi->insertCallsign(source, ret_id)) {
        setCallsignIdInMemcached(source, ret_id);
        _stats.sql_callsign.inserted++;
        _stompstats.sql_callsign.inserted++;
        return true;
      } // if

      if (_dbi->getCallsignId(source, ret_id)) {
        setCallsignIdInMemcached(source, ret_id);
        return true;
      } // if
      sleep(3);
    } // for

    _stats.sql_callsign.failed++;
    _stompstats.sql_callsign.failed++;

    return false;
  } // Store::getCallsignId

  std::string Store::getDirectionByCourse(const int course) {
      const char *dirs[] = { "north", "east", "south", "west", NULL };
      std::stringstream ret;

      int rounded = int(course / 22.5) % 16;
      if ((rounded % 4) == 0) {
        ret << dirs[rounded / 4];
      } // if
      else {
        ret << dirs[2 * int( floor( (( int(floor(rounded / 4)) + 1) % 4) / 2))];
        ret << "-" << dirs[1 + 2 * int(floor(rounded / 8))];
      }  // if

    return ret.str();
  } // Store::getDirectionBySource

  bool Store::getIconBySymbol(const std::string &symbol_table,
                              const std::string &symbol_code,
                              const int course,
                              Icon &icon) {
    md5wrapper md5;
    std::string key = md5.getHashFromString(symbol_table + symbol_code);
    std::string buf;

    // try and find in memcached
    if (getIconFromMemcached(key, buf)) {
      openframe::Vars v(buf);
      if (v.is("id,pa,ic,dir")) {
        std::string image = v["ic"];
        std::stringstream s;
        s << v["pa"];
        if (v["dir"] == "Y") {
          openframe::StringTool::replace(".png", "", image);
          s << "/compass/" << image << "-" << getDirectionByCourse(course) << ".png";
        } // if
        else s << "/" << image;

        icon.id = v["id"];
        icon.path = v["pa"];
        icon.image = v["ic"];
        icon.direction = v["dir"];
        icon.icon = s.str();

        return true;
      } // if
    } // if

    // not in memcached find in sql
    _stats.sql_icon.tries++;
    _stompstats.sql_icon.tries++;
    if (_dbi->getIconBySymbol(symbol_table, symbol_code, course, icon)) {
      setIconInMemcached(key, icon);
      _stats.sql_icon.hits++;
      _stompstats.sql_icon.hits++;
      return true;
    } // if

    _stats.sql_icon.misses++;
    _stompstats.sql_icon.misses++;


    _stats.sql_callsign.failed++;
    _stompstats.sql_callsign.failed++;

    return false;
  } // Store::getIconId

  bool Store::getNameId(const std::string &name, std::string &ret_id) {
    int i;

    // try and find in memcached
    if (getNameIdFromMemcached(name, ret_id)) return true;

    // not in memcached find in sql
    _stats.sql_name.tries++;
    _stompstats.sql_name.tries++;
    if (_dbi->getNameId(name, ret_id)) {
      setNameIdInMemcached(name, ret_id);
      _stats.sql_name.hits++;
      _stompstats.sql_name.hits++;
      return true;
    } // if
    _stats.sql_name.misses++;
    _stompstats.sql_name.misses++;

    // try again just in case another thread beat us
    for(i=0; i < 3; i++) {
      // not in sql try and create it
      if (_dbi->insertName(name, ret_id)) {
        setNameIdInMemcached(name, ret_id);
        _stats.sql_name.inserted++;
        _stompstats.sql_name.inserted++;
        return true;
      } // if

      if (_dbi->getNameId(name, ret_id)) {
        setNameIdInMemcached(name, ret_id);
        return true;
      } // if
      sleep(3);
    } // for

    _stats.sql_name.failed++;
    _stompstats.sql_name.failed++;

    return false;
  } // Store::getNameId

  bool Store::getDestId(const std::string &dest, std::string &ret_id) {
    int i;

    // try and find in memcached
    if (getDestIdFromMemcached(dest, ret_id)) return true;

    // not in memcached find in sql
    _stats.sql_dest.tries++;
    _stompstats.sql_dest.tries++;
    if (_dbi->getDestId(dest, ret_id)) {
      setDestIdInMemcached(dest, ret_id);
      _stats.sql_dest.hits++;
      _stompstats.sql_dest.hits++;
      return true;
    } // if
    _stats.sql_dest.misses++;
    _stompstats.sql_dest.misses++;

    // try again just in case another thread beat us
    for(i=0; i < 3; i++) {
      // not in sql try and create it
      if (_dbi->insertDest(dest, ret_id)) {
        setDestIdInMemcached(dest, ret_id);
        _stats.sql_dest.inserted++;
        _stompstats.sql_dest.inserted++;
        return true;
      } // if

      if (_dbi->getDestId(dest, ret_id)) {
        setDestIdInMemcached(dest, ret_id);
        return true;
      } // if
      sleep(3);
    } // for

    _stats.sql_dest.failed++;
    _stompstats.sql_dest.failed++;

    return false;
  } // Store::getDestId

  bool Store::getDigiId(const std::string &name, std::string &ret_id) {
    int i;

    // try and find in memcached
    if (getDigiIdFromMemcached(name, ret_id)) return true;

    // not in memcached find in sql
    _stats.sql_digi.tries++;
    _stompstats.sql_digi.tries++;
    if (_dbi->getDigiId(name, ret_id)) {
      setDigiIdInMemcached(name, ret_id);
      _stats.sql_digi.hits++;
      _stompstats.sql_digi.hits++;
      return true;
    } // if
    _stats.sql_digi.misses++;
    _stompstats.sql_digi.misses++;

    // try again just in case another thread beat us
    for(i=0; i < 3; i++) {
      // not in sql try and create it
      if (_dbi->insertDigi(name, ret_id)) {
        setDigiIdInMemcached(name, ret_id);
        _stats.sql_digi.inserted++;
        _stompstats.sql_digi.inserted++;
        return true;
      } // if

      if (_dbi->getDigiId(name, ret_id)) {
        setDigiIdInMemcached(name, ret_id);
        return true;
      } // if
      sleep(3);
    } // for

    _stats.sql_digi.failed++;
    _stompstats.sql_digi.failed++;

    return false;
  } // Store::getDigiId

  bool Store::setPacketId(const std::string &packetId, const std::string &callsignId) {
    openframe::Stopwatch sw;
    bool isOK = false;

    sw.Start();

    // try again just in case another thread beat us
    for(int i=0; i < 3; i++) {
      isOK = _dbi->insertPacket(packetId, callsignId);
      if (isOK) {
        _stats.sql_packet.inserted++;
        _stompstats.sql_packet.inserted++;
        break;
      } // if

      sleep(3);
    } // for

    if (!isOK) {
      _stats.sql_packet.failed++;
      _stompstats.sql_packet.failed++;
    } // if

    _profile->average("sql.insert.packet", sw.Time());

    return isOK;
  } // Store::setPacketId

  bool Store::getPacketId(const std::string &packetId) {
    return _dbi->getPacketId(packetId);
  } // Store::getPacketId

  bool Store::getPacketId(const std::string &callsignId, std::string &ret_id) {
    openframe::Stopwatch sw;
    bool isOK = false;

    sw.Start();

    // try again just in case another thread beat us
    for(int i=0; i < 3; i++) {
      isOK = _dbi->insertPacket(callsignId, ret_id);
      if (isOK) {
        _stats.sql_packet.inserted++;
        _stompstats.sql_packet.inserted++;
        break;
      } // if

      sleep(3);
    } // for

    if (!isOK) {
      _stats.sql_packet.failed++;
      _stompstats.sql_packet.failed++;
    } // if

    _profile->average("sql.insert.packet", sw.Time());

    return isOK;
  } // Store::getPacketId

  bool Store::getIconFromMemcached(const std::string &key, std::string &ret) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    if (!isMemcachedOk()) return false;

    _stats.cache_icon.tries++;
    _stompstats.cache_icon.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("icon", key, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.icon", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_icon.misses++;
      _stompstats.cache_icon.misses++;
      return false;
    } // if

    _stats.cache_icon.hits++;
    _stompstats.cache_icon.hits++;

    ret = buf;

    return true;
  } // getIconFromMemcached

  bool Store::setIconInMemcached(const std::string &key, const Icon &icon) {
    bool isOK = true;

    assert( key.length() );
    assert( icon.id.length() );
    assert( icon.path.length() );
    assert( icon.image.length() );
    assert( icon.direction.length() );

    if (!isMemcachedOk()) return false;

    openframe::Vars v;
    v.add("id", icon.id);
    v.add("pa", icon.path);
    v.add("ic", icon.image);
    v.add("dir", icon.direction);

    try {
      _memcached->put("icon", key, v.compile());
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_icon.stored++;
    _stompstats.cache_icon.stored++;

    return isOK;
  } // setIconInMemcached

  bool Store::getCallsignIdFromMemcached(const std::string &source, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;
    std::string key;

    if (!isMemcachedOk()) return false;

    _stats.cache_callsign.tries++;
    _stompstats.cache_callsign.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("callsign", openframe::StringTool::toUpper(source), buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.callsign", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_callsign.misses++;
      _stompstats.cache_callsign.misses++;
      return false;
    } // if

    _stats.cache_callsign.hits++;
    _stompstats.cache_callsign.hits++;

    ret_id = buf;

    return true;
  } // getCallsignIdFromMemcached

  bool Store::setCallsignIdInMemcached(const std::string &source, const std::string &id) {
    std::string key = openframe::StringTool::toUpper(source);
    bool isOK = true;

    assert( source.length() );
    assert( id.length() );

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("callsign", key, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_callsign.stored++;
    _stompstats.cache_callsign.stored++;

    return isOK;
  } // setCallsignIdInMemcached

  bool Store::getNameIdFromMemcached(const std::string &name, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    md5wrapper md5;
    std::string key = md5.getHashFromString( openframe::StringTool::toUpper(name) );

    if (!isMemcachedOk()) return false;

    _stats.cache_name.tries++;
    _stompstats.cache_name.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("objectname", key, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.name", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_name.misses++;
      _stompstats.cache_name.misses++;
      return false;
    } // if

    _stats.cache_name.hits++;
    _stompstats.cache_name.hits++;

    ret_id = buf;

    return true;
  } // getNameIdFromMemcached

  bool Store::setNameIdInMemcached(const std::string &name, const std::string &id) {
    assert( name.length() );
    assert( id.length() );

    md5wrapper md5;
    std::string key = md5.getHashFromString( openframe::StringTool::toUpper(name) );
    bool isOK = true;

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("objectname", key, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_name.stored++;
    _stompstats.cache_name.stored++;

    return isOK;
  } // setNameIdInMemcached

  bool Store::getMessageIdFromMemcached(const std::string &hash, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    if (!isMemcachedOk()) return false;

    _stats.cache_message.tries++;
    _stompstats.cache_message.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("message", hash, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.message", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_message.misses++;
      _stompstats.cache_message.misses++;
      return false;
    } // if

    _stats.cache_message.hits++;
    _stompstats.cache_message.hits++;

    ret_id = buf;

    return true;
  } // getMessageIdFromMemcached

  bool Store::setMessageIdInMemcached(const std::string &hash, const std::string &id) {
    assert( hash.length() );
    assert( id.length() );

    bool isOK = true;

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("message", hash, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_message.stored++;
    _stompstats.cache_message.stored++;

    return isOK;
  } // setMessageIdInMemcached

  bool Store::getDestIdFromMemcached(const std::string &dest, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;

    if (!isMemcachedOk()) return false;

    ++_stats.cache_dest.tries;
    ++_stompstats.cache_dest.tries;

    sw.Start();

    std::string buf;

    try {
      mcr = _memcached->get("dest", openframe::StringTool::toUpper(dest), buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.dest", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_dest.misses++;
      _stompstats.cache_dest.misses++;
      return false;
    } // if

    _stats.cache_dest.hits++;
    _stompstats.cache_dest.hits++;

    ret_id = buf;

    return true;
  } // getDestIdFromMemcached

  bool Store::setDestIdInMemcached(const std::string &dest, const std::string &id) {
    assert( dest.length() );
    assert( id.length() );

    bool isOK = true;

    if (!isMemcachedOk()) return false;

    std::string key = openframe::StringTool::toUpper(dest);
    try {
      _memcached->put("dest", key, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_dest.stored++;
    _stompstats.cache_dest.stored++;

    return isOK;
  } // setDestIdInMemcached

  bool Store::getDigiIdFromMemcached(const std::string &name, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;

    if (!isMemcachedOk()) return false;

    ++_stats.cache_digi.tries;
    ++_stompstats.cache_digi.tries;

    sw.Start();

    std::string buf;

    try {
      mcr = _memcached->get("digi", openframe::StringTool::toUpper(name), buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.digi", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_digi.misses++;
      _stompstats.cache_digi.misses++;
      return false;
    } // if

    _stats.cache_digi.hits++;
    _stompstats.cache_digi.hits++;

    ret_id = buf;

    return true;
  } // getDigiIdFromMemcached

  bool Store::setDigiIdInMemcached(const std::string &name, const std::string &id) {
    assert( name.length() );
    assert( id.length() );

    bool isOK = true;

    if (!isMemcachedOk()) return false;

    std::string key = openframe::StringTool::toUpper(name);
    try {
      _memcached->put("digi", key, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_digi.stored++;
    _stompstats.cache_digi.stored++;

    return isOK;
  } // setDigiIdInMemcached

  bool Store::getPathId(const std::string &path, std::string &ret_id) {
    md5wrapper md5;
    std::string hash = md5.getHashFromString(path);

    if (getPathIdFromMemcached(hash, ret_id)) return true;

    _stats.sql_path.tries++;
    _stompstats.sql_path.tries++;
    openframe::Stopwatch sw;
    sw.Start();
    bool ok = _dbi->getPathId(hash, ret_id);
    CALC_PROFILE(_stompstats.prof_sql_path, sw.Time());
    if (ok) {
      _stats.sql_path.hits++;
      _stompstats.sql_path.hits++;
      setPathIdInMemcached(hash, ret_id);
      return true;
    } // if
    _stats.sql_path.misses++;
    _stompstats.sql_path.misses++;

    // try again just in case another thread beat us
    for(int i=0; i < 3; i++) {
      if (_dbi->insertPath(hash, path, ret_id)) {
        _stats.sql_path.inserted++;
        _stompstats.sql_path.inserted++;
        setPathIdInMemcached(hash, ret_id);
        return true;
      } // if

      if (_dbi->getPathId(hash, ret_id)) {
        setPathIdInMemcached(hash, ret_id);
        return true;
      } // if
      sleep(3);
    } // for

    _stats.sql_path.failed++;
    _stompstats.sql_path.failed++;

    return false;
  } // Store::getPathId

  bool Store::getPathIdFromMemcached(const std::string &hash, std::string &ret_id) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    if (!isMemcachedOk())
      return false;

    _stats.cache_path.tries++;
    _stompstats.cache_path.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("path", hash, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _profile->average("memcached.path", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_path.misses++;
      _stompstats.cache_path.misses++;
      return false;
    } // if

    _stats.cache_path.hits++;
    _stompstats.cache_path.hits++;

    ret_id = buf;

    return true;
  } // Store::getPathIdFromMemcached

  bool Store::setPathIdInMemcached(const std::string &hash, const std::string &id) {
    assert( hash.length() );
    assert( id.length() );

    bool isOK = true;

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("path", hash, id);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      isOK = false;
    } // catch

    if (isOK) {
      _stats.cache_path.stored++;
      _stompstats.cache_path.stored++;
    } // if

    return isOK;
  } // Store::setPathIdInMemcached

  bool Store::setStatus(const std::string &packetId, const std::string &path) {

    // try again just in case another thread beat us
    for(int i=0; i < 3; i++) {
      if (_dbi->insertStatus(packetId, path)) {
        _stats.sql_status.inserted++;
        _stompstats.sql_status.inserted++;

        return true;
      } // if

      sleep(3);
    } // for

    _stats.sql_status.failed++;
    _stompstats.sql_status.failed++;

    return false;
  } // Store::setStatus

  //
  // Memcache Duplicates
  //
  bool Store::getDuplicateFromMemcached(const std::string &key, std::string &ret) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    if (!isMemcachedOk()) return false;

    _stats.cache_duplicates.tries++;
    _stompstats.cache_duplicates.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("duplicates", key, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.duplicates", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_duplicates.misses++;
      _stompstats.cache_duplicates.misses++;
      return false;
    } // if

    _stats.cache_duplicates.hits++;
    _stompstats.cache_duplicates.hits++;

    ret = buf;

    return true;
  } // getDuplicateFromMemcached

  bool Store::setDuplicateInMemcached(const std::string &key, const std::string &buf) {
    bool isOK = true;

    assert( key.length() );
    assert( buf.length() );

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("duplicates", key, buf, 3600);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_duplicates.stored++;
    _stompstats.cache_duplicates.stored++;

    return isOK;
  } // setDuplicateInMemcached

  //
  // Memcache Positions
  //
  bool Store::getPositionFromMemcached(const std::string &key, std::string &ret) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;
    std::string buf;

    if (!isMemcachedOk()) return false;

    _stats.cache_position.tries++;
    _stompstats.cache_position.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("position", key, buf);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.position", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_position.misses++;
      _stompstats.cache_position.misses++;
      return false;
    } // if

    _stats.cache_position.hits++;
    _stompstats.cache_position.hits++;

    ret = buf;

    return true;
  } // getPositionFromMemcached

  bool Store::setPositionInMemcached(const std::string &key, const std::string &buf) {
    bool isOK = true;

    if (!isMemcachedOk()) return false;

    try {
      _memcached->put("position", key, buf, 3600);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _stats.cache_positions.stored++;
    _stompstats.cache_positions.stored++;

    return isOK;
  } // setPositionInMemcached

  bool Store::setLocatorSeenInMemcached(const std::string &locator) {
    bool isOK = true;

    assert( locator.length() );
    std::string key = openframe::StringTool::toUpper(locator);

    if (!isMemcachedOk()) return false;

    openframe::Stopwatch sw;
    sw.Start();

    try {
      _memcached->put("locatorseen", key, openframe::stringify<time_t>( time(NULL) ) );
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _profile->average("memcached.locatorseen", sw.Time());
    CALC_PROFILE(_stompstats.prof_cache_locatorseen, sw.Time());

    _stats.cache_locatorseen.stored++;
    _stompstats.cache_locatorseen.stored++;

    return isOK;
  } // setLocatorSeenInMemcached

  bool Store::getLastpositionsFromMemcached(const std::string &locator, std::string &ret) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;

    if (!isMemcachedOk()) return false;

    _stats.cache_lastpositions.tries++;
    _stompstats.cache_lastpositions.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("lastpositions", openframe::StringTool::toUpper(locator), ret);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.lastpositions", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_lastpositions.misses++;
      _stompstats.cache_lastpositions.misses++;
      return false;
    } // if

    _stats.cache_lastpositions.hits++;
    _stompstats.cache_lastpositions.hits++;

    return true;
  } // getLastpotitionsFromMemcached

  bool Store::setLastpositionsInMemcached(aprs::APRS *aprs) {
    bool isOK = true;

    std::string source = aprs->getString("aprs.packet.source");
    std::string locator = aprs->getString("aprs.packet.position.maidenhead");

    assert( locator.length() );

    std::string key = openframe::StringTool::toUpper(locator);

    if (!isMemcachedOk()) return false;

    openframe::Stopwatch sw;
    sw.Start();

    std::string buf;
    bool found_locator = getLastpositionsFromMemcached(key, buf);

    // found locator, loop through to find this station
    // to replace, otherwise create new

    openframe::Vars va;
    std::string name_id =  aprs->isString("aprs.packet.object.name.id") ? aprs->getString("aprs.packet.object.name.id") : "0";
    va.add("id", aprs->getString("aprs.packet.id") );
    va.add("cid", aprs->getString("aprs.packet.callsign.id") );
    va.add("nid", name_id );
    va.add("sr", source );
    if (aprs->is_object())
      va.add("nm", aprs->getString("aprs.packet.object.name") );
    va.add("pa", aprs->getString("aprs.packet.path") );
    if (aprs->isString("aprs.packet.dirspd.direction"))
      va.add("cr", aprs->getString("aprs.packet.dirspd.direction") );
    if (aprs->isString("aprs.packet.dirspd.speed"))
      va.add("sp", aprs->getString("aprs.packet.dirspd.speed") );
    if (aprs->isString("aprs.packet.altitude"))
      va.add("at", aprs->getString("aprs.packet.altitude") );
    va.add("st", aprs->getString("aprs.packet.symbol.table") );
    va.add("sc", aprs->getString("aprs.packet.symbol.code") );
    if (aprs->isString("aprs.packet.symbol.overlay"))
      va.add("ovr", aprs->getString("aprs.packet.symbol.overlay") );
    va.add("phgr", aprs->getString("aprs.packet.phg.range") );
    va.add("phgd", aprs->getString("aprs.packet.phg.directivity") );
    va.add("ic", aprs->getString("aprs.packet.icon") );
    va.add("la", aprs->getString("aprs.packet.position.latitude.decimal") );
    va.add("ln", aprs->getString("aprs.packet.position.longitude.decimal") );
    va.add("ct", aprs->getString("aprs.packet.timestamp") );
    va.add("cm", aprs->getString("aprs.packet.comment") );
    std::string new_lastposition = va.compile();

    std::stringstream out;

    out << new_lastposition << std::endl;
    if (found_locator) {
      openframe::StreamParser sp = buf;
      std::string line;
      while( sp.sfind('\n', line) ) {
        openframe::Vars v(line);
        // invalid? skip!
        if ( !v.is("sr,ct") ) continue;

        time_t when = atoi( v["CT"].c_str() );
        // expire anything a day old or more than 100 positions
        if (when < time(NULL) - 86400) continue;

        if ( v["sr"] == source) continue;

        out << line << std::endl;
      } // while
    } // if

    try {
      _memcached->put("lastpositions", key, out.str() );
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message()
                     << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _profile->average("memcached.lastpositions", sw.Time());
    CALC_PROFILE(_stompstats.prof_cache_lastpositions, sw.Time());

    ++_stats.cache_lastpositions.stored;
    ++_stompstats.cache_lastpositions.stored;

    return isOK;
  } // setLastpositionsInMemcached

  bool Store::getPositionsFromMemcached(const std::string &source, std::string &ret) {
    MemcachedController::memcachedReturnEnum mcr;
    openframe::Stopwatch sw;

    if (!isMemcachedOk()) return false;

    _stats.cache_positions.tries++;
    _stompstats.cache_positions.tries++;

    sw.Start();

    try {
      mcr = _memcached->get("positions", openframe::StringTool::toUpper(source), ret);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message() << std::endl);
      _last_cache_fail_at = time(NULL);
    } // catch

    _profile->average("memcached.positions", sw.Time());

    if (mcr != MemcachedController::MEMCACHED_CONTROLLER_SUCCESS) {
      _stats.cache_positions.misses++;
      _stompstats.cache_positions.misses++;
      return false;
    } // if

    _stats.cache_positions.hits++;
    _stompstats.cache_positions.hits++;

    return true;
  } // getPositionsFromMemcached

  bool Store::setPositionsInMemcached(aprs::APRS *aprs) {
    bool isOK = true;

    // don't store positions for objects, WINLINK sends positions
    // like crazy and objects should replce each other not be tracked
    if ( aprs->isString("aprs.packet.position.posdup") || aprs->is_object() ) return false;

    std::string key = aprs->getString("aprs.packet.callsign.id");
    assert( key.length() );

    if (!isMemcachedOk()) return false;

    openframe::Stopwatch sw;
    sw.Start();

    std::string buf;
    bool found_positions = getPositionsFromMemcached(key, buf);

    openframe::Vars va;
    va.add("L", aprs->getString("aprs.packet.position.latitude.decimal") );
    va.add("G", aprs->getString("aprs.packet.position.longitude.decimal") );
    va.add("T", aprs->getString("aprs.packet.timestamp") );
    std::string new_position = va.compile();

    // found locator, loop through to find this station
    // to replace, otherwise create new
    std::stringstream out;
    out << new_position << std::endl;
    if (found_positions) {
      openframe::StreamParser sp = buf;
      std::string line;
      int c = 0;
      while( sp.sfind('\n', line) ) {
        openframe::Vars v(line);
        // invalid? skip!
        if ( !v.is("L,G,T") ) continue;

        time_t when = atoi( v["T"].c_str() );
        // expire anything a day old or more than 100 positions
        if (when < time(NULL) - 86400 || c > 100) break;

        out << line << std::endl;
        ++c;
      } // while
    } // if

    try {
      // expire positions after a day has passed
      _memcached->put("positions", key, out.str(), 86400);
    } // try
    catch(MemcachedController_Exception &e) {
      TLOG(LogError, << e.message() << std::endl);
      _last_cache_fail_at = time(NULL);
      return false;
    } // catch

    _profile->average("memcached.positions", sw.Time());
    CALC_PROFILE(_stompstats.prof_cache_positions, sw.Time());

    ++_stats.cache_positions.stored;
    ++_stompstats.cache_positions.stored;

    return isOK;
  } // setPositionsInMemcached

  bool Store::injectPosition(aprs::APRS *aprs) {
    setLastpositionsInMemcached(aprs);
    setPositionsInMemcached(aprs);

    return _dbi->position(aprs);
  } // Store::injectPosition

  bool Store::injectMessage(aprs::APRS *aprs) {
    return _dbi->message(aprs);
  } // Store::injectMessage

  bool Store::injectTelemtry(aprs::APRS *aprs) {
    return _dbi->telemetry(aprs);
  } // Store::injectTelemtry

  bool Store::injectRaw(aprs::APRS *aprs) {
    return _dbi->raw(aprs);
  } // Store::injectRaw

} // namespace openaprs
