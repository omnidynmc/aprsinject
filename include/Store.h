/**************************************************************************
 ** Dynamic Networking Solutions                                         **
 **************************************************************************
 ** OpenAPRS, mySQL APRS Injector                                        **
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
 $Id: DCC.h,v 1.8 2003/09/04 00:22:00 omni Exp $
 **************************************************************************/

#ifndef APRSINJECT_STORE_H
#define APRSINJECT_STORE_H

#include <openframe/openframe.h>
#include <openstats/StatsClient_Interface.h>

#include "DBI.h"

namespace aprsinject {

/**************************************************************************
 ** General Defines                                                      **
 **************************************************************************/

/**************************************************************************
 ** Structures                                                           **
 **************************************************************************/
  class MemcachedController;
  class Store : public openframe::LogObject,
                public openstats::StatsClient_Interface {
    public:
      static const time_t kDefaultReportInterval;

      Store(const openframe::LogObject::thread_id_t thread_id,
            const std::string &host,
            const std::string &user,
            const std::string &pass,
            const std::string &db,
            const std::string &memcached_host,
            const time_t expire_interval,
            const time_t report_interval=kDefaultReportInterval);
      virtual ~Store();
      Store &init();
      void onDescribeStats();
      void onDestroyStats();

      void try_stats();

      bool getCallsignId(const std::string &source, std::string &ret_id);
      bool getNameId(const std::string &source, std::string &ret_id);
      bool getIconBySymbol(const std::string &symbol_table, const std::string &symbol_code, const int course, Icon &icon);
      bool getDestId(const std::string &dest, std::string &ret_id);
      bool getDigiId(const std::string &name, std::string &ret_id);
      bool getMessageId(const std::string &name, std::string &ret_id);
      bool getPathId(const std::string &hash, std::string &);
      bool getStatusId(const std::string &hash, std::string &);
      bool setPacketId(const std::string &packetId, const std::string &callsignId);
      bool getPacketId(const std::string &callsignId, std::string &ret_id);
      bool getPacketId(const std::string &packetId);
      bool getDuplicateFromMemcached(const std::string &hash, std::string &buf);
      bool setDuplicateInMemcached(const std::string &hash, const std::string &buf);
      bool getPositionFromMemcached(const std::string &hash, std::string &buf);
      bool setPositionInMemcached(const std::string &hash, const std::string &buf);
      bool setLocatorSeenInMemcached(const std::string &locator);
      bool getLastpositionsFromMemcached(const std::string &locaator, std::string &ret);
      bool setLastpositionsInMemcached(aprs::APRS *aprs);
      bool getPositionsFromMemcached(const std::string &source, std::string &ret);
      bool setPositionsInMemcached(aprs::APRS *aprs);

      // FIXME
      std::string getDirectionByCourse(const int course);

      // injection members
      bool injectPosition(aprs::APRS *aprs);
      bool injectMessage(aprs::APRS *aprs);
      bool injectTelemtry(aprs::APRS *aprs);
      bool injectRaw(aprs::APRS *aprs);

    // ### Variables ###

    protected:
      void try_stompstats();
      bool isMemcachedOk() const { return _last_cache_fail_at < time(NULL) - 60; }
      bool getCallsignIdFromMemcached(const std::string &source, std::string &ret_id);
      bool setCallsignIdInMemcached(const std::string &source, const std::string &id);
      bool getIconFromMemcached(const std::string &key, std::string &ret);
      bool setIconInMemcached(const std::string &key, const Icon &icon);
      bool getNameIdFromMemcached(const std::string &name, std::string &ret_id);
      bool setNameIdInMemcached(const std::string &name, const std::string &id);
      bool getDestIdFromMemcached(const std::string &dest, std::string &ret_id);
      bool setDestIdInMemcached(const std::string &dest, const std::string &id);
      bool getDigiIdFromMemcached(const std::string &name, std::string &ret_id);
      bool setDigiIdInMemcached(const std::string &name, const std::string &id);
      bool getMessageIdFromMemcached(const std::string &name, std::string &ret_id);
      bool setMessageIdInMemcached(const std::string &name, const std::string &id);
      bool getPathIdFromMemcached(const std::string &hash, std::string &ret_id);
      bool setPathIdInMemcached(const std::string &hash, const std::string &id);
      bool getStatusIdFromMemcached(const std::string &hash, std::string &ret_id);
      bool setStatusIdInMemcached(const std::string &hash, const std::string &id);

    private:
      DBI *_dbi;			// new Injection handler
      MemcachedController *_memcached;	// memcached controller instance
      openframe::Stopwatch *_profile;

      // contructor vars
      std::string _host;
      std::string _user;
      std::string _pass;
      std::string _db;
      std::string _memcached_host;
      time_t _expire_interval;
      time_t _last_cache_fail_at;

      struct profile_stats_t {
        int mean;
        int count;
      }; // profile_stats_t

      struct memcache_stats_t {
        unsigned int hits;
        unsigned int misses;
        unsigned int tries;
        unsigned int stored;
      }; // memcache_stats_t

      struct sql_stats_t {
        unsigned int hits;
        unsigned int misses;
        unsigned int tries;
        unsigned int inserted;
        unsigned int failed;
      };

      struct obj_stats_t {
        memcache_stats_t cache_store;
        memcache_stats_t cache_callsign;
        memcache_stats_t cache_dest;
        memcache_stats_t cache_digi;
        memcache_stats_t cache_icon;
        memcache_stats_t cache_message;
        memcache_stats_t cache_name;
        memcache_stats_t cache_packet;
        memcache_stats_t cache_path;
        memcache_stats_t cache_status;
        memcache_stats_t cache_duplicates;
        memcache_stats_t cache_position;
        memcache_stats_t cache_locatorseen;
        memcache_stats_t cache_lastpositions;
        memcache_stats_t cache_positions;
        sql_stats_t sql_store;
        sql_stats_t sql_callsign;
        sql_stats_t sql_dest;
        sql_stats_t sql_digi;
        sql_stats_t sql_icon;
        sql_stats_t sql_message;
        sql_stats_t sql_name;
        sql_stats_t sql_packet;
        sql_stats_t sql_path;
        sql_stats_t sql_position;
        sql_stats_t sql_status;
        profile_stats_t prof_cache_locatorseen;
        profile_stats_t prof_cache_lastpositions;
        profile_stats_t prof_cache_positions;
        profile_stats_t prof_sql_message;
        profile_stats_t prof_sql_path;
        time_t last_report_at;
        time_t report_interval;
        time_t created_at;
      } _stats;
      obj_stats_t _stompstats;

      void init_stats(obj_stats_t &stats, const bool startup=false);
}; // Store

/**************************************************************************
 ** Macro's                                                              **
 **************************************************************************/

#define CALC_PROFILE(a, b) { \
  ++a.count; \
  int oldmean = a.mean; \
  int ms = static_cast<int>(b * 1000000.0); \
  a.mean += (ms - oldmean) / a.count; \
}

/**************************************************************************
 ** Proto types                                                          **
 **************************************************************************/
} // namespace aprsinject
#endif
