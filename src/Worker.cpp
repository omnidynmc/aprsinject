#include "config.h"

#include <string>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <openframe/openframe.h>
#include <stomp/StompHeaders.h>
#include <stomp/StompFrame.h>
#include <stomp/Stomp.h>
#include <aprs/aprs.h>

#include <Worker.h>
#include <Store.h>
#include <MemcachedController.h>
#include <DBI.h>

namespace aprsinject {
  using namespace openframe::loglevel;

  const int Worker::kDefaultStompPrefetch	= 1024;
  const time_t Worker::kDefaultStatsInterval	= 3600;
  const time_t Worker::kDefaultMemcachedExpire	= 3600;
  const char *Worker::kStompDestErrors		= "/topic/feeds.aprs.is.errors";
  const char *Worker::kStompDestRejects		= "/topic/feeds.aprs.is.rejects";
  const char *Worker::kStompDestDuplicates	= "/topic/feeds.aprs.is.duplicates";
  const char *Worker::kStompDestNotifyMessages	= "/topic/notify.aprs.messages";

  Worker::Worker(const openframe::LogObject::thread_id_t thread_id,
                 const std::string &stomp_hosts,
                 const std::string &stomp_dest,
                 const std::string &stomp_login,
                 const std::string &stomp_passcode,
                 const std::string &memcached_host,
                 const std::string &db_host,
                 const std::string &db_user,
                 const std::string &db_pass,
                 const std::string &db_database,
                 const bool drop_defer)
         : openframe::LogObject(thread_id),
           _stomp_hosts(stomp_hosts),
           _stomp_dest(stomp_dest),
           _stomp_login(stomp_login),
           _stomp_passcode(stomp_passcode),
           _memcached_host(memcached_host),
           _db_host(db_host),
           _db_user(db_user),
           _db_pass(db_pass),
           _db_database(db_database),
           _drop_defer(drop_defer) {

    _store = NULL;
    _stomp = NULL;
    _profile = NULL;
    _connected = false;
    _console = false;
    _locators_intval = new openframe::Intval(5);

    init_stats(_stats, true);
    init_stompstats(_stompstats, true);
    _stats.report_interval = 60;
    _stompstats.report_interval = 5;
  } // Worker::Worker

  Worker::~Worker() {
    onDestroyStats();

    while( !_results.empty() ) {
      Result *result = _results.front();
      result->release();
      _results.pop_front();
    } // while

    delete _locators_intval;
    if (_store) delete _store;
    if (_stomp) delete _stomp;
    if (_profile) delete _profile;
  } // Worker:~Worker

  void Worker::init() {
    try {
      stomp::StompHeaders *headers = new stomp::StompHeaders("openstomp.prefetch",
                                                             openframe::stringify<int>(kDefaultStompPrefetch)
                                                            );
      headers->add_header("heart-beat", "0,5000");
      _stomp = new stomp::Stomp(_stomp_hosts,
                                _stomp_login,
                                _stomp_passcode,
                                headers);

      _store = new Store(thread_id(),
                         _db_host,
                         _db_user,
                         _db_pass,
                         _db_database,
                         _memcached_host,
                         kDefaultMemcachedExpire,
                         kDefaultStatsInterval);
      _store->replace_stats( stats(), "");
      _store->set_elogger( elogger(), elog_name() );
      _store->init();
    } // try
    catch(std::bad_alloc &xa) {
      assert(false);
    } // catch

    _profile = new openframe::Stopwatch();
    _profile->add("time.loop.handle", 300);
    _profile->add("time.aprs.parse", 300);

  } // Worker::init

  void Worker::init_stats(obj_stats_t &stats, const bool startup) {
    stats.connects = 0;
    stats.disconnects = 0;
    stats.packets = 0;
    stats.age = 0;
    stats.frames_in = 0;
    stats.frames_out = 0;

    stats.last_report_at = time(NULL);
    if (startup) stats.created_at = time(NULL);
  } // Worker::init_stats

  void Worker::init_stompstats(obj_stompstats_t &stats, const bool startup) {
    memset(&stats.aprs_stats, 0, sizeof(aprs_stats_t) );

    stats.last_report_at = time(NULL);
    if (startup) stats.created_at = time(NULL);
  } // Worker::init_stompstats

  void Worker::onDescribeStats() {
    describe_stat("num.frames.out", "worker"+thread_id_str()+"/num frames out", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_stat("num.frames.in", "worker"+thread_id_str()+"/num frames in", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_stat("num.bytes.out", "worker"+thread_id_str()+"/num bytes out", openstats::graphTypeCounter, openstats::dataTypeInt);
    describe_stat("num.bytes.in", "worker"+thread_id_str()+"/num bytes in", openstats::graphTypeCounter, openstats::dataTypeInt);

    describe_stat("num.work.in", "worker"+thread_id_str()+"/num work in", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.work.out", "worker"+thread_id_str()+"/work out", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.result.queue", "worker"+thread_id_str()+"/num result queue", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.aprs.rejects", "worker"+thread_id_str()+"/aprs rejects", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.aprs.duplicates", "worker"+thread_id_str()+"/aprs duplicates", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.aprs.position.error", "worker"+thread_id_str()+"/aprs position errors", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.aprs.deferred", "worker"+thread_id_str()+"/aprs deferred", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.aprs.errors", "worker"+thread_id_str()+"/aprs errors", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.sql.inserted", "worker"+thread_id_str()+"/sql inserted", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("num.sql.failed", "worker"+thread_id_str()+"/sql failed", openstats::graphTypeGauge, openstats::dataTypeInt, openstats::useTypeSum);
    describe_stat("time.run", "worker"+thread_id_str()+"/run loop time", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);
    describe_stat("time.run.handle", "worker"+thread_id_str()+"/run handle time", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);
    describe_stat("time.aprs.parse", "worker"+thread_id_str()+"/aprs parse time", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);
    describe_stat("time.sql.insert", "worker"+thread_id_str()+"/sql insert time", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);
    describe_stat("time.write.event", "worker"+thread_id_str()+"/write event time", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);

    // APRS Packet Stats
    describe_stat("aprs_stats.rate.age", "aprs stats/rate/age", openstats::graphTypeGauge, openstats::dataTypeFloat, openstats::useTypeMean);
    describe_stat("aprs_stats.rate.packet", "aprs stats/rate/packet", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.position", "aprs stats/rate/positions", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.message", "aprs stats/rate/message", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.telemetry", "aprs stats/rate/telemetry", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.status", "aprs stats/rate/status", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.capabilities", "aprs stats/rate/capabilities", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.peet_logging", "aprs stats/rate/peet_logging", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.weather", "aprs stats/rate/weather", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.dx", "aprs stats/rate/dx", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.experimental", "aprs stats/rate/experimental", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.beacon", "aprs stats/rate/beacon", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.unknown", "aprs stats/rate/unknown", openstats::graphTypeCounter);

    describe_stat("aprs_stats.rate.reject.invparse", "aprs stats/rate/reject/invparse", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.reject.duplicate", "aprs stats/rate/reject/duplicate", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.reject.tofast", "aprs stats/rate/reject/tofast", openstats::graphTypeCounter);
    describe_stat("aprs_stats.rate.reject.tosoon", "aprs stats/rate/reject/tosoon", openstats::graphTypeCounter);

    describe_stat("aprs_stats.ratio.position", "aprs stats/ratio/positions", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.message", "aprs stats/ratio/message", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.telemetry", "aprs stats/ratio/telemetry", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.status", "aprs stats/ratio/status", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.capabilities", "aprs stats/ratio/capabilities", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.peet_logging", "aprs stats/ratio/peet_logging", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.weather", "aprs stats/ratio/weather", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.dx", "aprs stats/ratio/dx", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.experimental", "aprs stats/ratio/experimental", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.beacon", "aprs stats/ratio/beacon", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.unknown", "aprs stats/ratio/unknown", openstats::graphTypeGauge, openstats::dataTypeFloat);

    describe_stat("aprs_stats.ratio.reject.invparse", "aprs stats/ratio/reject/invparse", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.reject.duplicate", "aprs stats/ratio/reject/duplicate", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.reject.tosoon", "aprs stats/ratio/reject/too soon", openstats::graphTypeGauge, openstats::dataTypeFloat);
    describe_stat("aprs_stats.ratio.reject.tofast", "aprs stats/ratio/reject/too fast", openstats::graphTypeGauge, openstats::dataTypeFloat);
  } // Worker::onDescribeStats

  void Worker::onDestroyStats() {
    destroy_stat("*");
  } // Worker::onDestroyStats

  void Worker::try_stats() {
    try_stompstats();

    if (_stats.last_report_at > time(NULL) - _stats.report_interval) return;

    int diff = time(NULL) - _stats.last_report_at;
    double pps = double(_stats.packets) / diff;
    double fps_in = double(_stats.frames_in) / diff;
    double fps_out = double(_stats.frames_out) / diff;
    time_t age = _stats.age / _stats.packets;

    TLOG(LogNotice, << "Stats packets " << _stats.packets
                    << ", pps " << pps << "/s"
                    << ", frames in " << _stats.frames_in
                    << ", fps in " << fps_in << "/s"
                    << ", frames out " << _stats.frames_out
                    << ", fps out " << fps_out << "/s"
                    << ", age " << age << "s"
                    << ", next in " << _stats.report_interval
                    << ", connect attempts " << _stats.connects
                    << "; " << _stomp->connected_to()
                    << std::endl);

    init_stats(_stats);
    _stats.last_report_at = time(NULL);
  } // Worker::try_stats

  void Worker::try_stompstats() {
    if (_stompstats.last_report_at > time(NULL) - _stompstats.report_interval) return;
    datapoint_float("aprs_stats.rate.age", (_stompstats.aprs_stats.age / _stompstats.aprs_stats.packet));

    datapoint_float("time.run.handle", _profile->average("time.loop.handle"));
    datapoint_float("time.aprs.parse", _profile->average("time.aprs.parse"));

    datapoint("aprs_stats.rate.packet", _stompstats.aprs_stats.packet);
    datapoint("aprs_stats.rate.position", _stompstats.aprs_stats.position);
    datapoint("aprs_stats.rate.message", _stompstats.aprs_stats.message);
    datapoint("aprs_stats.rate.telemetry", _stompstats.aprs_stats.telemetry);
    datapoint("aprs_stats.rate.status", _stompstats.aprs_stats.status);
    datapoint("aprs_stats.rate.capabilities", _stompstats.aprs_stats.capabilities);
    datapoint("aprs_stats.rate.peet_logging", _stompstats.aprs_stats.peet_logging);
    datapoint("aprs_stats.rate.weather", _stompstats.aprs_stats.weather);
    datapoint("aprs_stats.rate.dx", _stompstats.aprs_stats.dx);
    datapoint("aprs_stats.rate.experimental", _stompstats.aprs_stats.experimental);
    datapoint("aprs_stats.rate.beacon", _stompstats.aprs_stats.beacon);
    datapoint("aprs_stats.rate.unknown", _stompstats.aprs_stats.unknown);

    datapoint("aprs_stats.rate.reject.invparse", _stompstats.aprs_stats.reject_invparse);
    datapoint("aprs_stats.rate.reject.duplicate", _stompstats.aprs_stats.reject_duplicate);
    datapoint("aprs_stats.rate.reject.tofast", _stompstats.aprs_stats.reject_tofast);
    datapoint("aprs_stats.rate.reject.tosoon", _stompstats.aprs_stats.reject_tosoon);

    datapoint_float("aprs_stats.ratio.position", OPENSTATS_PERCENT(_stompstats.aprs_stats.position, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.message", OPENSTATS_PERCENT(_stompstats.aprs_stats.message, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.telemetry", OPENSTATS_PERCENT(_stompstats.aprs_stats.telemetry, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.status", OPENSTATS_PERCENT(_stompstats.aprs_stats.status, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.capabilities", OPENSTATS_PERCENT(_stompstats.aprs_stats.capabilities, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.peet_logging", OPENSTATS_PERCENT(_stompstats.aprs_stats.peet_logging, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.weather", OPENSTATS_PERCENT(_stompstats.aprs_stats.weather, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.dx", OPENSTATS_PERCENT(_stompstats.aprs_stats.dx, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.experimental", OPENSTATS_PERCENT(_stompstats.aprs_stats.experimental, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.beacon", OPENSTATS_PERCENT(_stompstats.aprs_stats.beacon, _stompstats.aprs_stats.packet) );

    datapoint_float("aprs_stats.ratio.reject.invparse", OPENSTATS_PERCENT(_stompstats.aprs_stats.reject_invparse, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.reject.duplicate", OPENSTATS_PERCENT(_stompstats.aprs_stats.reject_duplicate, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.reject.tofast", OPENSTATS_PERCENT(_stompstats.aprs_stats.reject_tofast, _stompstats.aprs_stats.packet) );
    datapoint_float("aprs_stats.ratio.reject.tosoon", OPENSTATS_PERCENT(_stompstats.aprs_stats.reject_tosoon, _stompstats.aprs_stats.packet) );

    init_stompstats(_stompstats);
  } // Worker::try_stompstats

  bool Worker::run() {
    try_stats();
    _store->try_stats();
    try_locators();

    handle_results();

    /**********************
     ** Check Connection **
     **********************/
    if (!_connected) {
      ++_stats.connects;
      bool ok = _stomp->subscribe(_stomp_dest, "1");
      if (!ok) {
        TLOG(LogInfo, << "not connected, retry in 2 seconds; " << _stomp->last_error() << std::endl);
        sleep(2);
        return false;
      } // if
      _connected = true;
      TLOG(LogNotice, << "Connected to " << _stomp->connected_to() << std::endl);
    } // if

    stomp::StompFrame *frame;
    bool ok = false;

    try {
      ok = _stomp->next_frame(frame);
    } // try
    catch(stomp::Stomp_Exception &ex) {
      TLOG(LogWarn, << "ERROR: " << ex.message() << std::endl);
      _connected = false;
      ++_stats.disconnects;
      return false;
    } // catch

    if (!ok) return false;

    /*******************
     ** Process Frame **
     *******************/
    ++_stats.frames_in;
    bool is_usable = frame->is_command(stomp::StompFrame::commandMessage)
                     && frame->is_header("message-id");
    if (!is_usable) {
      frame->release();
      return true;
    } // if
    ++_stats.frames_in;

    openframe::StreamParser sp = frame->body();
    std::string packet;
    while( sp.sfind('\n', packet) ) {
      ++_stats.packets;
      openframe::StreamParser spp = packet;

      // find timestamp
      std::string aprs_created_str;
      if (!spp.sfind(' ', aprs_created_str)) continue; // skip this line
      time_t aprs_created = atoi( aprs_created_str.c_str() );

      _stats.age += abs(time(NULL) - aprs_created);
      _stompstats.aprs_stats.age += abs(time(NULL) - aprs_created);

      Result *result = create_result(spp.str(), aprs_created);

      // add to process list
      if (result) _results.push_back(result);
    } // while

    std::string message_id = frame->get_header("message-id");
    _stomp->ack(message_id, "1");

    frame->release();
    return true;
  } // Worker::run

  Result *Worker::create_result(const std::string &body, const time_t timestamp) {
    Result *result;
    try {
      result = new Result(body, time(NULL) );
    } // try
    catch(std::bad_alloc &xa) {
      assert(false);
    } // catch

    openframe::Stopwatch sw;
    try {
      sw.Start();
      result->_aprs = new aprs::APRS(body, timestamp);
      _profile->average("time.aprs.parse", sw.Time());
    } // try
    catch(aprs::APRS_Exception &e) {
      result->_aprs = NULL;
      result->_error = e.message();
      result->_status = Result::statusRejected;
      _stompstats.aprs_stats.reject_invparse++;
      post_error(kStompDestErrors, body, result);
      print_result(result);

      result->release();
      return NULL;
    } // catch

    // at this point we've parsed ok, if anything else resets this
    // then the packet wasn't ok
    result->_status = Result::statusOk;

    return result;
  } // Worker::create_result

  size_t Worker::handle_results() {
    /*****************************
     ** Handle Incoming Packets **
     *****************************/
    openframe::Stopwatch sw;
    size_t num_handled = 0;
    int retries = 0;
    while(!_results.empty() && num_handled < 100 && retries < 3) {
      Result *result = _results.front();

      sw.Start();
      bool ok = handle(result);
      _profile->average("time.loop.handle", sw.Time());

      print_result(result);

      if (ok) {
        _results.pop_front();
        result->release();
        ++num_handled;
      } // if
      else {
        TLOG(LogWarn, << "Errors detected while handling result, try #" << retries+1 << std::endl);
        bool shouldDrop = _drop_defer && result->is_status(Result::statusDeferred) && retries < 2;
        if (shouldDrop) {
          _results.pop_front();
          result->release();

          ++num_handled;

          continue;
        } // if

        retries++;
        sleep(3);
      } // else
    } // while

    return num_handled;
  } // Worker::handle_results

  bool Worker::handle(Result *result) {
    assert(result != NULL);
    aprs::APRS *aprs = result->aprs();
    assert(aprs != NULL);

    bool ok = result->is_status(Result::statusDeferred);
    // only check for dups if we're not deferred
    if (!ok) {
      ok = !checkForDuplicates(result) && !checkForPositionErrors(result);
      // either this was a duplicate or a position error, in which case
      // we will do nothing for this packet
      if (!ok) {
        if (!result->_error.length() && result->_aprs->isString("aprs.packet.error.message"))
          result->_error = result->_aprs->getString("aprs.packet.error.message");
        post_error(kStompDestRejects, aprs->packet(), result);
        return true;
      } // if
    } // if

    // at this point we're past rejecting let's get all of our
    // needed ids in order to proceed
    ok = preprocess(result);
    if (!ok) {
      TLOG(LogWarn, << "Errors detected while preprocessing result; "
                    << result->_error << std::endl);
      return false;
    } // if

    // try and inject record
    ok = inject(result);
    if (!ok) return false;

    // we don't care if this succeeded we're good to go
    // ... for now
    process(result);

    return true;
  } // Worker::handle

  bool Worker::preprocess(Result *result) {
    assert(result != NULL);
    aprs::APRS *aprs = result->aprs();
    assert(aprs != NULL);

    // take care of callsign id
    std::string callsignId;
    bool ok = _store->getCallsignId(result->_aprs->source(), callsignId);
    if (!ok) {
      result->_status = Result::statusDeferred;
      result->_error = "could not get callsign id";
      return false;
    } // if
    aprs->replaceString("aprs.packet.callsign.id", callsignId);

    // take care of callsign id
    ok = aprs->isString("aprs.packet.symbol.table") && aprs->isString("aprs.packet.symbol.code");
    if (ok) {
      Icon icon;
      int course = (aprs->isString("aprs.packet.dirspd.direction") ? atoi(aprs->getString("aprs.packet.dirspd.direction").c_str()) : 0);
      ok = _store->getIconBySymbol(aprs->getString("aprs.packet.symbol.table"), aprs->getString("aprs.packet.symbol.code"), course, icon);
      if (ok) {
        aprs->replaceString("aprs.packet.icon.id", icon.id);
        aprs->replaceString("aprs.packet.icon", icon.icon);
      } // if
      else {
        result->_status = Result::statusDeferred;
        result->_error = "could not get icon id for "
                         + aprs->getString("aprs.packet.symbol.table")
                         + aprs->getString("aprs.packet.symbol.code");
        return false;
      } // else
    } // if

    // take care of packet id
    std::string packetId;
    ok = _store->getPacketId(callsignId, packetId);
    if (!ok) {
      result->_status = Result::statusDeferred;
      result->_error = "could not get packet id";

      return false;
    } // if

    aprs->replaceString("aprs.packet.id", packetId);

    // take care of path id
    ok = _store->setPath(packetId, result->_aprs->path());
    if (!ok) {
      result->_status = Result::statusDeferred;
      result->_error = "could not get path id";
      return false;
    } // if

    // take care of dest id
    std::string destId;
    std::string dest = aprs->getString("aprs.packet.path0");
    ok = _store->getDestId(dest, destId);
    if (!ok) {
      result->_status = Result::statusDeferred;
      result->_error = "could not get destination id";
      return false;
    } // if
    aprs->replaceString("aprs.packet.destination.id", destId);

    if (result->_aprs->isString("aprs.packet.object.name")) {
      std::string nameId;
      ok = _store->getNameId( result->_aprs->getString("aprs.packet.object.name") , nameId);
      if (!ok) {
        result->_status = Result::statusDeferred;
        result->_error = "could not get name id";
        return false;
      } // if
      aprs->replaceString("aprs.packet.object.name.id", nameId);
    } // if

    // if we're a position or status report we'll have some additional text as a comment
    if (result->_aprs->packetType() == aprs::APRS::APRS_PACKET_POSITION) {
      ok = _store->setStatus(packetId, result->_aprs->status());
      if (!ok) {
        result->_status = Result::statusDeferred;
        result->_error = "could not set status";
        return false;
      } // if

      if (aprs->isString("aprs.packet.position.maidenhead")) {
        // take care of digi id
        std::string maidenheadId;
        std::string locator = aprs->getString("aprs.packet.position.maidenhead");
        ok = _store->getMaidenheadId(locator, maidenheadId);
        if (!ok) {
          result->_status = Result::statusDeferred;
          result->_error = "could not get maidenhead id";
          return false;
        } // if

        aprs->replaceString("aprs.packet.position.maidenhead.sql.id", maidenheadId);
      } // if
    } // if

    // if we're a message we need to find the to callsign
    if (result->_aprs->packetType() == aprs::APRS::APRS_PACKET_MESSAGE) {
      std::string targetId;
      std::string target = result->_aprs->getString("aprs.packet.message.target");
      ok = _store->getCallsignId(target, targetId);
      if (!ok) {
        result->_status = Result::statusDeferred;
        result->_error = "could not get message target callsign id";
        return false;
      } // if
      aprs->replaceString("aprs.packet.message.target.id", targetId);
    } // if

    // work on digipath
    for(int i=1; i < 9; i++) {
      std::string digi = "aprs.packet.path" + openframe::stringify<int>(i);
      std::string name = aprs->getString(digi);

      if ( !name.length() ) {
        aprs->replaceString(digi + ".id", "0");
        continue;
      } // if

      // take care of digi id
      std::string digiId;
      ok = _store->getDigiId(name, digiId);
      if (!ok) {
        result->_status = Result::statusDeferred;
        result->_error = "could not get digi id for path " + openframe::stringify<int>(i);
        return false;
      } // if
      aprs->replaceString(digi + ".id", digiId);
    } // for

    return result;
  } // Worker::preprocess

  bool Worker::inject(Result *result) {
    assert(result != NULL);
    aprs::APRS *aprs = result->aprs();
    assert(aprs != NULL);

    bool ok = _store->injectRaw(aprs);
    if (!ok) {
      result->_status = Result::statusDeferred;
      result->_error = "could not inject raw";
      return false;
    } // if

    switch(aprs->packetType()) {
      case aprs::APRS::APRS_PACKET_POSITION:
        ok = _store->injectPosition(aprs);
        if (!ok) {
          result->_status = Result::statusDeferred;
          result->_error = "could not inject position";
          return false;
        } // if

        _locators.insert( aprs->getString("aprs.packet.position.maidenhead") );
        break;
      case aprs::APRS::APRS_PACKET_MESSAGE:
        ok = _store->injectMessage(aprs);
        if (!ok) {
          result->_status = Result::statusDeferred;
          result->_error = "could not inject message";
          return false;
        } // if
        break;
      case aprs::APRS::APRS_PACKET_TELEMETRY:
        ok = _store->injectTelemtry(aprs);
        if (!ok) {
          result->_status = Result::statusDeferred;
          result->_error = "could not inject telemtry";
          return false;
        } // if
        break;
      default:
        break;
    } // switch

    return result;
  } // Worker::inject

  void Worker::post_error(const char *dest, const std::string &packet, const Result *result) {
    std::string status;
    switch( result->status() ) {
      case Result::statusRejected:
        status = "rejected";
        datapoint("num.aprs.rejects", 1);
        break;
      case Result::statusDuplicate:
        status = "duplicate";
        datapoint("num.aprs.duplicates", 1);
        break;
      case Result::statusDeferred:
        status = "deferred";
        datapoint("num.aprs.deferred", 1);
        break;
      case Result::statusPositError:
        status = "position error";
        datapoint("num.aprs.position.error", 1);
        break;
      case Result::statusNone:
      default:
        assert(false);		// bug?
        break;
    } // switch

    std::stringstream out;
    out << "{ \"packet\"  : \"" << openframe::StringTool::escape(packet) << "\"," << std::endl
        << "  \"error\"   : \"" << openframe::StringTool::escape(result->_error) << "\"," << std::endl
        << "  \"status\"  : \"" << status << "\"," << std::endl
        << "  \"created\" : \"" << time(NULL) << "\" }"
        << std::endl;

    _stomp->send( dest, out.str() );
  } // Worker::post_error

  bool Worker::checkForDuplicates(Result *result) {
    aprs::APRS *aprs = result->_aprs;
    openframe::Vars *v;
    md5wrapper md5;

    std::string body = openframe::StringTool::toLower(aprs->source() + ":" + aprs->body());
    std::string key = md5.getHashFromString(body);

    bool is_dup = false;
    std::string buf;
    bool found = _store->getDuplicateFromMemcached(key, buf);

    if (found) {
      TLOG(LogDebug, << "memcached{dup} found key " << key << std::endl);
      TLOG(LogDebug, << "memcached{dup} body: " << body << std::endl);
      TLOG(LogDebug, << "memcached{dup} data: " << buf);

      v = new openframe::Vars(buf);
      bool exists = v->exists("ct");
      if (exists) {
        time_t diff = time(NULL) - atoi( (*v)["ct"].c_str() );
        if (diff < 30) {
          result->_status = Result::statusDuplicate;
          _stompstats.aprs_stats.reject_duplicate++;
          post_error(kStompDestDuplicates, result->_aprs->body(), result);
          is_dup = true;
        } // if
      } // if
      else assert(false);	// bug
      delete v;
    } // if
    else {
      // not found must add it
      v = new openframe::Vars();

      v->add("sr", result->_aprs->source());
      v->add("ct", result->_aprs->getString("aprs.packet.timestamp"));
      if (result->_aprs->packetType() == aprs::APRS::APRS_PACKET_POSITION) {
        v->add("la", result->_aprs->getString("aprs.packet.position.latitude.decimal"));
        v->add("ln", result->_aprs->getString("aprs.packet.position.longitude.decimal"));
      } // if

      v->compile(buf, "");
      delete v;

      _store->setDuplicateInMemcached(key, buf);
    } // else

    return is_dup;
  } // Worker::checkForDuplicates

  bool Worker::checkForPositionErrors(Result *result) {
    aprs::APRS *aprs = result->_aprs;
    openframe::Vars *v;
    md5wrapper md5;

    if (aprs->packetType() != aprs::APRS::APRS_PACKET_POSITION
        || aprs->is_object())
      // is not a position or is an object which will be automated and
      // can present multiple positions by same source quickly
      return false;

    std::string key = openframe::StringTool::toLower(aprs->source());
    std::string buf;

    bool found = _store->getPositionFromMemcached(key, buf);
    bool is_posit_error = false;

    std::string comment = md5.getHashFromString(aprs->getString("aprs.packet.comment"));
    if (found) {
      // do position err checks
      v = new openframe::Vars(buf);

      // is it a position we can use?
      if (v->exists("la,ln,ct,cm")) {
        double tlat = atof((*v)["la"].c_str());
        double tlng = atof((*v)["ln"].c_str());
        time_t ct = atol((*v)["ct"].c_str());

        double distance = aprs::APRS::calcDistance(tlat, tlng, aprs->lat(), aprs->lng(), 'M');

        // packets can arrive out of order which can cause a negative timestamp
        // since we only want the time difference take absolute value
        time_t diff = abs(aprs->timestamp() - ct);

        // pos dups tells the injector not to add to positions table
        // this should catch repeaters and other fixed stations (like WX) from
        // clogging up the positions table; we want to catch fast reports
        // and reports that are less than 0.1 miles.
        if (diff < 1 || distance < 0.1) aprs->addString("aprs.packet.position.posdup", "1");

        double speed = aprs::APRS::calcSpeed(distance, diff, 8, 1);

        if (diff < 5 && comment == (*v)["cm"]) {
          // probably don't want to do this, catches digis that advertise
          // two packets with different comment content
          TLOG(LogDebug, << "memcached{pos}, found key " << key << std::endl);
          TLOG(LogDebug, << "memcached{pos}, data: " << buf);
          TLOG(LogDebug, << "memcached{pos}, pos: "
                        << aprs->lat() << "," << aprs->lng()
                        << std::endl);
          TLOG(LogDebug, << "memcached{pos}, lame: " << diff << "seconds"
                        << std::endl);
          _stompstats.aprs_stats.reject_tosoon++;
          aprs->addString("aprs.packet.error.message", "position: tx < 5 seconds (" + openframe::stringify<time_t>(diff) + ")");
          is_posit_error = true;
        } // if
        else if (speed > 500 && comment == (*v)["cm"]) {
          TLOG(LogDebug, << "memcached{pos}, found key " << key << std::endl);
          TLOG(LogDebug, << "memcached{pos}, data: " << buf << std::endl);
          TLOG(LogDebug, << "memcached{pos}, pos: " << aprs->lat() << "," << aprs->lng()
                        << std::endl);
          TLOG(LogDebug, << "memcached{pos}, lame: speed " << speed << std::endl);
          _stompstats.aprs_stats.reject_tofast++;
          aprs->addString("aprs.packet.error.message", "position: gps glitch speed > 500");
          is_posit_error = true;
        } // else if

      } // if

      delete v;

      if (is_posit_error) result->_status = Result::statusPositError;
    } // if

    if ( result->is_status(Result::statusOk) ) {
      v = new openframe::Vars();

      v->add("sr", aprs->source());
      v->add("la", aprs->getString("aprs.packet.position.latitude.decimal"));
      v->add("ln", aprs->getString("aprs.packet.position.longitude.decimal"));
      v->add("ct", aprs->getString("aprs.packet.timestamp"));
      v->add("cm", comment);

      v->compile(buf, "");
      delete v;

      //_logf("key: %s, not found", result->_aprs->source().c_str());

      _store->setPositionInMemcached(key, buf);
    } // if

    return is_posit_error;
  } // Worker::checkForPositionErrors

  void Worker::process(Result *result) {
    assert(result != NULL);		// bug
    aprs::APRS *aprs = result->aprs();
    assert(aprs != NULL);		// bug

    _stompstats.aprs_stats.packet++;
    switch(aprs->packetType()) {
      case aprs::APRS::APRS_PACKET_POSITION:
        _stompstats.aprs_stats.position++;
        break;
      case aprs::APRS::APRS_PACKET_MESSAGE: {
          openframe::Vars *v = new openframe::Vars();
          v->add("ct", aprs->getString("aprs.packet.timestamp") );
          v->add("sr", aprs->getString("aprs.packet.source") );
          v->add("to", aprs->getString("aprs.packet.message.target") );
          v->add("ms", aprs->getString("aprs.packet.message.text") );
          v->add("pa", aprs->getString("aprs.packet.path") );
          bool has_id = aprs->isString("aprs.packet.message.id");
          if (has_id) v->add("id", aprs->getString("aprs.packet.message.id") );

          bool has_ack = aprs->isString("aprs.packet.message.ack");
          if (has_ack) v->add("ack", aprs->getString("aprs.packet.message.ack") );

          bool has_rpl = aprs->isString("aprs.packet.message.id.reply");
          if (has_rpl) v->add("rpl", aprs->getString("aprs.packet.message.id.reply") );

          bool has_ackonly = aprs->isString("aprs.packet.message.ackonly");
          if (has_ackonly) v->add("ao", aprs->getString("aprs.packet.message.ackonly") );

          _stomp->send(kStompDestNotifyMessages, v->compile() );
          ++_stats.frames_out;
          delete v;
        }
        _stompstats.aprs_stats.message++;
        break;
      case aprs::APRS::APRS_PACKET_TELEMETRY:
        _stompstats.aprs_stats.telemetry++;
        break;
      case aprs::APRS::APRS_PACKET_STATUS:
        _stompstats.aprs_stats.status++;
        break;
      case aprs::APRS::APRS_PACKET_CAPABILITIES:
        _stompstats.aprs_stats.capabilities++;
        break;
      case aprs::APRS::APRS_PACKET_PEETLOGGING:
        _stompstats.aprs_stats.peet_logging++;
        break;
      case aprs::APRS::APRS_PACKET_WEATHER:
        _stompstats.aprs_stats.weather++;
        break;
      case aprs::APRS::APRS_PACKET_DX:
        _stompstats.aprs_stats.dx++;
        break;
      case aprs::APRS::APRS_PACKET_EXPERIMENTAL:
        _stompstats.aprs_stats.experimental++;
        break;
      case aprs::APRS::APRS_PACKET_BEACON:
        _stompstats.aprs_stats.beacon++;
        break;
      case aprs::APRS::APRS_PACKET_UNKNOWN:
        _stompstats.aprs_stats.unknown++;
        break;
      default:
        break;
    } // switch
  } // Worker::process

  void Worker::print_result(Result *result) {
    assert(result != NULL);
    aprs::APRS *aprs = result->aprs();

    if (_console) {
      if (aprs == NULL)
        std::cout << "x";
      else if (result->is_status(Result::statusDuplicate))
        std::cout << "=";
      else if (result->is_status(Result::statusRejected))
        std::cout << "r";
      else if (result->is_status(Result::statusDeferred))
        std::cout << "!";
      else if (result->is_status(Result::statusPositError))
        std::cout << "p";
      else if ( aprs->isString("aprs.packet.position.posdup") )
        std::cout << ",";
      else
        std::cout << ".";

      std::cout.flush();
    } // if
  } // Worker::print_result

  void Worker::try_locators() {
    if ( !_locators_intval->is_next() ) return;

    for(locators_citr citr = _locators.begin(); citr != _locators.end(); citr++)
      _store->setLocatorSeenInMemcached(*citr);

    _locators.clear();
  } // worker::try_locators
} // namespace aprsinject
