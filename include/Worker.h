#ifndef APRSINJECT_WORKER_H
#define APRSINJECT_WORKER_H

#include <string>
#include <vector>
#include <list>

#include <openframe/openframe.h>
#include <openstats/openstats.h>
#include <stomp/Stomp.h>
#include <aprs/APRS.h>

namespace aprsinject {
/**************************************************************************
 ** General Defines                                                      **
 **************************************************************************/

/**************************************************************************
 ** Structures                                                           **
 **************************************************************************/

  class MemcachedController;
  class DBI_Inject;
  class Store;

  class Work {
    public:
      Work(time_t timestamp, const std::string packet) : _timestamp(timestamp), _packet(packet) { }

      std::string packet() const { return _packet; }
      time_t timestamp() const { return _timestamp; }

    private:
      time_t _timestamp;
      std::string _packet;
  }; // class Work

  class Result : public openframe::Refcount {
    public:
      enum statusEnum {
        statusNone		= 0,
        statusRejected		= 1,
        statusDuplicate		= 2,
        statusDeferred		= 3,
        statusPositError	= 4,
        statusOk		= 5
      }; // statusEnum

      Result(const std::string &packet, const time_t now) :
             _aprs(NULL),
             _ack(false),
             _parseTime(0.0), _packet(packet), _timestamp(now), _status(statusNone) { }
      virtual ~Result() {
        if (_aprs) delete _aprs;
      } // Result

      friend class Worker;

      time_t timestamp() const { return _timestamp; }
      double parseTime() const { return _parseTime; }
      std::string error() const { return _error; }
      std::string packet() const { return _packet; }
      bool ack() const { return _ack; }
      statusEnum status() const { return _status; }
      bool is_status(statusEnum st) const { return _status == st; }
      aprs::APRS *aprs() const { return _aprs; }

    private:
      aprs::APRS *_aprs;
      bool _ack;
      double _parseTime;
      std::string _packet;
      std::string _error;
      time_t _timestamp;
      statusEnum _status;
  };

  class Worker_Exception : public openframe::OpenFrame_Exception {
    public:
      Worker_Exception(const std::string message) throw() : openframe::OpenFrame_Exception(message) { };
  }; // class Worker_Exception

  class Worker : public virtual openframe::LogObject,
                 public openstats::StatsClient_Interface {
    public:
      // ### Constants ### //
      static const int kDefaultStompPrefetch;
      static const time_t kDefaultStatsInterval;
      static const time_t kDefaultMemcachedExpire;
      static const char *kStompDestErrors;
      static const char *kStompDestRejects;
      static const char *kStompDestDuplicates;
      static const char *kStompDestNotifyMessages;

      // ### Init ### //
      Worker(const openframe::LogObject::thread_id_t thread_id,
             const std::string &stomp_hosts,
             const std::string &stomp_dest,
             const std::string &stomp_login,
             const std::string &stomp_passcode,
             const std::string &memcached_host,
             const std::string &db_host,
             const std::string &db_user,
             const std::string &db_pass,
             const std::string &db_database,
             const bool drop_defer = true);
      virtual ~Worker();
      void init();
      bool run();
      void try_stats();
      void try_locators();

      // ### Type Definitions ###
      typedef std::deque<Work *> work_t;
      typedef work_t::iterator work_itr;
      typedef work_t::const_iterator work_citr;
      typedef work_t::size_type work_st;

      typedef std::deque<Result *> results_t;
      typedef results_t::iterator results_itr;
      typedef results_t::const_iterator results_citr;
      typedef results_t::size_type results_st;

      typedef std::set<std::string> locators_t;
      typedef locators_t::iterator locators_itr;
      typedef locators_t::const_iterator locators_citr;
      typedef locators_t::size_type locators_st;

      // ### Options ### //
      Worker &set_console(const bool onoff) {
        _console = onoff;
        return *this;
      } // set_console

      // ### StatsClient Pure Virtuals ### //
      void onDescribeStats();
      void onDestroyStats();

    protected:
      void try_stompstats();
      Result *create_result(const std::string &body, const time_t timestamp);
      void print_result(Result *result);
      size_t handle_results();
      bool handle(Result *);
      bool preprocess(Result *);
      bool inject(Result *);
      void process(Result *);
      bool checkForDuplicates(Result *);
      bool checkForPositionErrors(Result *);
      void post_error(const char *dest, const std::string &packet, const Result *result);

    private:
      // constructor variables
      std::string _stomp_hosts;
      std::string _stomp_dest;
      std::string _stomp_login;
      std::string _stomp_passcode;
      std::string _memcached_host;
      std::string _db_host;
      std::string _db_user;
      std::string _db_pass;
      std::string _db_database;
      bool _drop_defer;

      openframe::Stopwatch *_profile;

      Store *_store;
      stomp::Stomp *_stomp;

      work_t _work;
      results_t _results;
      locators_t _locators;

      openframe::Intval *_locators_intval;

      bool _connected;
      bool _console;

      struct aprs_stats_t {
        unsigned int packet;
        unsigned int position;
        unsigned int message;
        unsigned int telemetry;
        unsigned int status;
        unsigned int capabilities;
        unsigned int peet_logging;
        unsigned int weather;
        unsigned int dx;
        unsigned int experimental;
        unsigned int beacon;
        unsigned int unknown;
        time_t age;
        unsigned int reject_invparse;
        unsigned int reject_duplicate;
        unsigned int reject_tosoon;
        unsigned int reject_tofast;
      }; // aprs_stats_t

      struct obj_stats_t {
        unsigned int connects;
        unsigned int disconnects;
        unsigned int packets;
        unsigned int frames_in;
        unsigned int frames_out;
        time_t age;
        time_t report_interval;
        time_t last_report_at;
        time_t created_at;
      } _stats;
      void init_stats(obj_stats_t &stats, const bool startup = false);

      struct obj_stompstats_t {
        aprs_stats_t aprs_stats;
        time_t report_interval;
        time_t last_report_at;
        time_t created_at;
      } _stompstats;
      void init_stompstats(obj_stompstats_t &stats, const bool startup = false);
  }; // class Worker

/**************************************************************************
 ** Macro's                                                              **
 **************************************************************************/

/**************************************************************************
 ** Proto types                                                          **
 **************************************************************************/
} // namespace aprsinject
#endif
