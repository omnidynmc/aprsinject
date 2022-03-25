/**************************************************************************
 ** Dynamic Networking Solutions                                         **
 **************************************************************************
 ** OpenAPRS, Internet APRS MySQL Injector                               **
 ** Copyright (C) 1999 Gregory A. Carter                                 **
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
 $Id: Vars.cpp,v 1.1 2005/11/21 18:16:04 omni Exp $
 **************************************************************************/

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <new>
#include <iostream>
#include <string>
#include <exception>
#include <sstream>

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <ctype.h>
#include <math.h>

#include <openframe/openframe.h>
#include <aprs/APRS.h>

#include "DBI.h"
#include "Validator.h"

namespace aprsinject {
  using namespace openframe::loglevel;

  /**************************************************************************
   ** DBI Class                                                     **
   **************************************************************************/

  /******************************
   ** Constructor / Destructor **
   ******************************/

  DBI::DBI(const openframe::LogObject::thread_id_t thread_id,
                         const std::string &db,
                         const std::string &host,
                         const std::string &user,
                         const std::string &pass)
             : openframe::LogObject(thread_id),
               openframe::DBI(db, host, user, pass) {
  } // DBI::DBI

  DBI::~DBI() {
  } // DBI::~DBI

  void DBI::prepare_queries() {
    add_query("i_last_position",
      "INSERT INTO last_position (packet_id,    callsign_id,    name_id,    icon_id, latitude, longitude, create_ts) VALUES \
                                 (%0:packet_id, %1:callsign_id, %2:name_id, %3q:icon_id, %4:latitude, %5:longitude, %6:create_ts)     \
       ON DUPLICATE KEY UPDATE \
       packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id), icon_id=VALUES(icon_id),\
       latitude=VALUES(latitude), longitude=VALUES(longitude), create_ts=VALUES(create_ts)"
    );

  } // DBI::prepare_queries

  bool DBI::position(aprs::APRS *aprs) {
    assert(aprs != NULL);

    Validator validator;

    std::string packet_id = aprs->getString("aprs.packet.id");
    std::string callsign_id = aprs->getString("aprs.packet.callsign.id");
    std::string name_id = aprs->isString("aprs.packet.object.name.id") ? aprs->getString("aprs.packet.object.name.id") : "0";
    std::string icon_id = aprs->getString("aprs.packet.icon.id");

    bool ok = false;
    try {
      mysqlpp::Transaction trans(*_sqlpp);
      mysqlpp::Query query = _sqlpp->query();

      //
      // query for last_position
      //
      q("i_last_position")->execute(packet_id,
                                    callsign_id,
                                    name_id,
                                    icon_id,
                                    aprs->getString("aprs.packet.position.maidenhead"),
                                    aprs->latitude(),
                                    aprs->longitude(),
                                    aprs->timestamp()
                                   );
      query = _sqlpp->query();

      //
      // query for last_position_meta
      //
//      mysqlpp::Null<std::string> dir = mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.dirspd.direction");
      query << "INSERT INTO last_position_meta (packet_id, callsign_id, name_id, dest_id, "
            <<                                 "course, speed, altitude, symbol_table,"
            <<                                 "symbol_code, overlay, `range`, type, weather, telemetry,"
            <<                                 "position_type_id, mbits, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << name_id
            << "," << aprs->getString("aprs.packet.destination.id")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dirspd.direction")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dirspd.speed")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.altitude")
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.symbol.table")
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.symbol.code")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:1", aprs, "aprs.packet.symbol.overlay")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.rng")
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.object.type")
            << "," << mysqlpp::quote << (aprs->isString("aprs.packet.weather") ? 'Y' : 'N')
            << "," << mysqlpp::quote << (aprs->isString("aprs.packet.telemetry") ? 'Y' : 'N')
            << "," << aprs->getString("aprs.packet.position.type.id")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:3", aprs, "aprs.packet.mic_e.raw.mbits")
            << "," << aprs->timestamp()
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id), dest_id=VALUES(dest_id),"
            << "course=VALUES(course), speed=VALUES(speed), altitude=VALUES(altitude),"
            << "symbol_table=VALUES(symbol_table), symbol_code=VALUES(symbol_code), overlay=VALUES(overlay),"
            << "`range`=VALUES(`range`), type=VALUES(type), weather=VALUES(weather), telemetry=VALUES(telemetry), position_type_id=VALUES(position_type_id), mbits=VALUES(mbits),"
            << "create_ts=VALUES(create_ts)";
      query.execute();
      query = _sqlpp->query();

      //
      // query for last_phg
      //
      if (aprs->isString("aprs.packet.phg.power")) {
        query << "INSERT INTO last_phg (packet_id, callsign_id, name_id, power, haat, gain, `range`,"
              <<                       "direction, beacon, create_ts) VALUES"
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << name_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.power")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.haat")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.gain")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.range")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.phg.directivity")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.phg.beacon")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id),"
              << "power=VALUES(power), haat=VALUES(haat), gain=VALUES(gain), `range`=VALUES(`range`),"
              << "direction=VALUES(direction), beacon=VALUES(beacon), create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // if

      //
      // query for last_dfr
      //
      if (aprs->isString("aprs.packet.dfr.bearing")) {
        query << "INSERT INTO last_dfr (packet_id, callsign_id, name_id, bearing, hits, `range`,"
              <<                       "quality, create_ts) VALUES"
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << name_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dfr.bearing")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dfr.hits")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.dfr.range")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dfr.quality")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id),"
              << "bearing=VALUES(bearing), hits=VALUES(hits), `range`=VALUES(`range`), quality=VALUES(quality),"
              << "create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // if

      //
      // query for last_dfs
      //
      if (aprs->isString("aprs.packet.dfs.power")) {
        query << "INSERT INTO last_dfs (packet_id, callsign_id, name_id, power, haat, gain, `range`,"
              <<                       "direction, create_ts) VALUES"
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << name_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.power")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.haat")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.gain")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.phg.range")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.phg.directivity")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id),"
              << "power=VALUES(power), haat=VALUES(haat), gain=VALUES(gain), `range`=VALUES(`range`),"
              << "direction=VALUES(direction), create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // if

      //
      // query for last_frequency
      //
      if (aprs->isString("aprs.packet.afrs.frequency")) {
        query << "INSERT INTO last_frequency (packet_id, callsign_id, name_id, frequency, `range`,"
              <<                             "range_east, tone, afrs_type, receive, alternate, type, create_ts) VALUES"
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << name_id
              << "," << mysqlpp::quote << aprs->getString("aprs.packet.afrs.frequency")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.afrs.range")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.afrs.range.east")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:6", aprs, "aprs.packet.afrs.tone")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.afrs.type")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:7", aprs, "aprs.packet.afrs.frequency.receive")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:7", aprs, "aprs.packet.afrs.frequency.alternate")
              << "," << mysqlpp::quote << aprs->getString("aprs.packet.object.type")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), name_id=VALUES(name_id),"
              << "frequency=VALUES(frequency), `range`=VALUES(`range`), range_east=VALUES(range_east), tone=VALUES(tone),"
              << "afrs_type=VALUES(afrs_type), receive=VALUES(receive), alternate=VALUES(alternate), type=VALUES(type),"
              << "create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // if

      if ( !aprs->isString("aprs.packet.position.posdup") &&
           !aprs->is_object() ) {
        //
        // query for position
        //
        query << "INSERT INTO position (packet_id, callsign_id, maidenhead_id, latitude, longitude, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << aprs->getString("aprs.packet.position.maidenhead.sql.id")
              << "," << aprs->latitude()
              << "," << aprs->longitude()
              << "," << aprs->timestamp()
              << ")";
        query.execute();
        query = _sqlpp->query();

        //
        // query for position_meta
        //
        query << "INSERT INTO position_meta (packet_id, "
              <<                             "course, speed, altitude, symbol_table, symbol_code, time_of_fix,"
              <<                             "create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dirspd.direction")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.dirspd.speed")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.altitude")
              << "," << mysqlpp::quote << aprs->getString("aprs.packet.symbol.table")
              << "," << mysqlpp::quote << aprs->getString("aprs.packet.symbol.code")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.timestamp")
              << "," << aprs->timestamp()
              << ")";
        query.execute();
        query = _sqlpp->query();
      } // if (!posdup)

      // Is the packet broadcasting weather information?
      if (aprs->getString("aprs.packet.weather").length() > 0) {
        //
        // query for last_weather
        //
        query << "INSERT INTO last_weather (packet_id, callsign_id, latitude, longitude,"
              <<                           "wind_direction, wind_speed, wind_gust, temperature, rain_hour,"
              <<                           "rain_calendar_day, rain_24hour_day, humidity, barometer,"
              <<                           "luminosity, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << aprs->latitude()
              << "," << aprs->longitude()
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.direction")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.speed")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.gust")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.temperature.celcius")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.hour")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.midnight")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.24hour")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int|maxval:100", aprs, "aprs.packet.weather.humidity")
              << "," << std::fixed << std::setprecision(2) << atof( aprs->getString("aprs.packet.weather.pressure").c_str() ) // FIXME: no need to divide
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.luminosity.wsm")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id),"
              << "latitude=VALUES(latitude), longitude=VALUES(longitude),"
              << "wind_direction=VALUES(wind_direction), wind_speed=VALUES(wind_speed), wind_gust=VALUES(wind_gust),"
              << "temperature=VALUES(temperature), rain_hour=VALUES(rain_hour), rain_calendar_day=VALUES(rain_calendar_day),"
              << "rain_24hour_day=VALUES(rain_24hour_day), humidity=VALUES(humidity), barometer=VALUES(barometer),"
              << "luminosity=VALUES(luminosity), create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();

        query << "INSERT INTO weather (packet_id, callsign_id, wind_direction, wind_speed, wind_gust,"
              <<                      "temperature, rain_hour, rain_calendar_day, rain_24hour_day, humidity, barometer,"
              <<                      "luminosity, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.direction")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.speed")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.wind.gust")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.temperature.celcius")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.hour")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.midnight")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.weather.rain.24hour")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int|maxval:100", aprs, "aprs.packet.weather.humidity")
              << "," << std::fixed << std::setprecision(2) << (atof(aprs->getString("aprs.packet.weather.pressure").c_str())) // FIXME: no need to divide
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.weather.luminosity.wsm")
              << "," << aprs->timestamp()
              << ")";
        query.execute();
        query = _sqlpp->query();
      } // if

      trans.commit();
      ok = true;
    } // try (transaction)
    catch(const mysqlpp::BadQuery &e) {

      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::position}: "
                       << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::position}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::position}: "
                       << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::position}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return ok;
  } // DBI::position

  bool DBI::message(aprs::APRS *aprs) {
    assert(aprs != NULL);

    Validator validator;

    std::string packet_id = aprs->getString("aprs.packet.id");
    std::string callsign_id = aprs->getString("aprs.packet.callsign.id");

    bool ok = false;
    try {
      mysqlpp::Transaction trans(*_sqlpp);
      mysqlpp::Query query = _sqlpp->query();

      //
      // query for message_meta
      //
      query << "INSERT INTO message (packet_id, callsign_id, callsign_to_id, `body`, msgid, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.message.target.id")
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.message.text")
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.message.id")
            << "," << aprs->timestamp()
            << ")";
      query.execute();
      query = _sqlpp->query();

      //
      // query for last_message
      //
      query << "INSERT INTO last_message (packet_id, callsign_id, callsign_to_id, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << aprs->getString("aprs.packet.message.target.id")
            << "," << aprs->timestamp()
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), callsign_to_id=VALUES(callsign_to_id),"
            << "create_ts=VALUES(create_ts)";
      query.execute();
      query = _sqlpp->query();


      //
      // query for last_bulletin
      //
      openframe::StringTool::regexMatchListType regexList;
      if (openframe::StringTool::ereg("^((BLN[0-9A-Z]{1,6})|(NWS-[0-9A-Z]{1,5}))$",
                                      aprs->getString("aprs.packet.message.target"),
                                      regexList)) {
        query << "INSERT INTO last_bulletin (packet_id, callsign_id, addressee, text, id, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.message.target")
            << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.message.text")
            << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.message.id")
            << ", UNIX_TIMESTAMP()"
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), addressee=VALUES(addressee),"
            << "text=VALUES(text), id=VALUES(id), create_ts=VALUES(create_ts)";

      } // if

      if (aprs->getString("aprs.packet.telemetry.message.type") == "EQNS") {
        query << "INSERT INTO telemetry_eqns (packet_id, callsign_id, a_0, b_0, c_0, a_1, b_1, c_1, a_2,"
              <<                             "b_2, c_2, a_3, b_3, c_3, a_4, b_4, c_4, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a0.a")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a0.b")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a0.c")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a1.a")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a1.b")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a1.c")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a2.a")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a2.b")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a2.c")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a3.a")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a3.b")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a3.c")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a4.a")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a4.b")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.a4.c")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), a_0=VALUES(a_0),"
              << "b_0=VALUES(b_0), c_0=VALUES(c_0), a_1=VALUES(a_1), b_1=VALUES(b_1), c_1=VALUES(c_1), a_2=VALUES(a_2), b_2=VALUES(b_2),"
              << "c_2=VALUES(c_2), a_3=VALUES(a_3), b_3=VALUES(b_3), c_3=VALUES(c_3), a_4=VALUES(a_4), b_4=VALUES(b_4), c_4=VALUES(c_4),"
              << "create_ts=VALUES(create_ts)";

        query.execute();
        query = _sqlpp->query();
      } // if
    else if (aprs->getString("aprs.packet.telemetry.message.type") == "UNIT") {
        query << "INSERT INTO telemetry_unit (packet_id, callsign_id, a_0, a_1, a_2, a_3, a_4, d_0, d_1,"
              <<                             "d_2, d_3, d_4, d_5, d_6, d_7, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:7", aprs, "aprs.packet.telemetry.analog0")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:6", aprs, "aprs.packet.telemetry.analog1")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:5", aprs, "aprs.packet.telemetry.analog2")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:6", aprs, "aprs.packet.telemetry.analog3")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:4", aprs, "aprs.packet.telemetry.analog4")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:5", aprs, "aprs.packet.telemetry.digital0")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:4", aprs, "aprs.packet.telemetry.digital1")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:3", aprs, "aprs.packet.telemetry.digital2")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:3", aprs, "aprs.packet.telemetry.digital3")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:3", aprs, "aprs.packet.telemetry.digital4")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:2", aprs, "aprs.packet.telemetry.digital5")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:2", aprs, "aprs.packet.telemetry.digital6")
              << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:2", aprs, "aprs.packet.telemetry.digital7")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), a_0=VALUES(a_0),"
              << "a_1=VALUES(a_1), a_2=VALUES(a_2), a_3=VALUES(a_3), a_4=VALUES(a_4), d_0=VALUES(d_0), d_1=VALUES(d_1), d_2=VALUES(d_2),"
              << "d_3=VALUES(d_3), d_4=VALUES(d_4), d_5=VALUES(d_5), d_6=VALUES(d_6), d_7=VALUES(d_7),"
              << "create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // else if
      else if (aprs->getString("aprs.packet.telemetry.message.type") == "PARM") {
        query << "INSERT INTO telemetry_parm (packet_id, callsign_id, a_0, a_1, a_2, a_3, a_4, d_0, d_1,"
              <<                              "d_2, d_3, d_4, d_5, d_6, d_7, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.analog0")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.analog1")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.analog2")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.analog3")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.analog4")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital0")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital1")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital2")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital3")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital4")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital5")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital6")
              << "," << mysqlpp::quote << NULL_OPTIONPP(aprs, "aprs.packet.telemetry.digital7")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), a_0=VALUES(a_0),"
              << "a_1=VALUES(a_1), a_2=VALUES(a_2), a_3=VALUES(a_3), a_4=VALUES(a_4), d_0=VALUES(d_0), d_1=VALUES(d_1), d_2=VALUES(d_2),"
              << "d_3=VALUES(d_3), d_4=VALUES(d_4), d_5=VALUES(d_5), d_6=VALUES(d_6), d_7=VALUES(d_7),"
              << "create_ts=VALUES(create_ts)";
        query.execute();
        query = _sqlpp->query();
      } // else if
      else if (aprs->getString("aprs.packet.telemetry.message.type") == "BITS") {
        query << "INSERT INTO telemetry_bits (packet_id, callsign_id, bitsense, project_title, create_ts) VALUES "
              << "(" << mysqlpp::quote << packet_id
              << "," << callsign_id
              << "," << aprs->getString("aprs.packet.telemetry.bitsense")
              << "," << mysqlpp::quote << aprs->getString("aprs.packet.telemetry.project")
              << "," << aprs->timestamp()
              << ") ON DUPLICATE KEY UPDATE "
              << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), bitsense=VALUES(bitsense),"
              << "project_title=VALUES(project_title), create_ts=VALUES(create_ts)";

        query.execute();
        query = _sqlpp->query();
      } // else if


      trans.commit();
      ok = true;
    } // try (transaction)
    catch(const mysqlpp::BadQuery &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::message}: " << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::message}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::message}: "
                      << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::message}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return ok;
  } // DBI::message

  bool DBI::raw(aprs::APRS *aprs) {
    assert(aprs != NULL);

    std::string packet_id = aprs->getString("aprs.packet.id");
    std::string callsign_id = aprs->getString("aprs.packet.callsign.id");

    bool ok = false;
    try {
      mysqlpp::Transaction trans(*_sqlpp);
      mysqlpp::Query query = _sqlpp->query();

      //
      // query for last_message
      //
      query << "INSERT INTO last_raw (packet_id, callsign_id, "
            <<                       "information, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.raw")
            << ", UNIX_TIMESTAMP()"
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id),"
            << "information=VALUES(information),"
            << "create_ts=VALUES(create_ts)";
      query.execute();
      query = _sqlpp->query();

      //
      // query for last_message
      //
      query << "INSERT INTO last_raw_meta (packet_id, callsign_id, dest_id, digi0_id, digi1_id,"
            <<                       "digi2_id, digi3_id, digi4_id, digi5_id, digi6_id,"
            <<                       "digi7_id, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << aprs->getString("aprs.packet.destination.id")
            << "," << aprs->getString("aprs.packet.path1.id")
            << "," << aprs->getString("aprs.packet.path2.id")
            << "," << aprs->getString("aprs.packet.path3.id")
            << "," << aprs->getString("aprs.packet.path4.id")
            << "," << aprs->getString("aprs.packet.path5.id")
            << "," << aprs->getString("aprs.packet.path6.id")
            << "," << aprs->getString("aprs.packet.path7.id")
            << "," << aprs->getString("aprs.packet.path8.id")
            << ", UNIX_TIMESTAMP()"
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id), dest_id=VALUES(dest_id),"
            << "digi0_id=VALUES(digi0_id), digi1_id=VALUES(digi1_id), digi2_id=VALUES(digi2_id),"
            << "digi3_id=VALUES(digi3_id), digi4_id=VALUES(digi4_id), digi5_id=VALUES(digi5_id),"
            << "digi6_id=VALUES(digi6_id), digi7_id=VALUES(digi7_id),"
            << "create_ts=VALUES(create_ts)";
      query.execute();
      query = _sqlpp->query();

      //
      // query for raw
      //
      query << "INSERT INTO raw (packet_id, callsign_id, information, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << aprs->getString("aprs.packet.raw")
            << "," << aprs->timestamp()
            << ")";
      query.execute();
      query = _sqlpp->query();

      //
      // query for raw_meta
      //
      query << "INSERT INTO raw_meta (packet_id, callsign_id, dest_id,"
            <<                       "digi0_id, digi1_id, digi2_id, digi3_id, digi4_id,"
            <<                       "digi5_id, digi6_id, digi7_id, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << aprs->getString("aprs.packet.destination.id")
            << "," << aprs->getString("aprs.packet.path1.id")
            << "," << aprs->getString("aprs.packet.path2.id")
            << "," << aprs->getString("aprs.packet.path3.id")
            << "," << aprs->getString("aprs.packet.path4.id")
            << "," << aprs->getString("aprs.packet.path5.id")
            << "," << aprs->getString("aprs.packet.path6.id")
            << "," << aprs->getString("aprs.packet.path7.id")
            << "," << aprs->getString("aprs.packet.path8.id")
            << "," << aprs->timestamp()
            << ")";
      query.execute();
      query = _sqlpp->query();

      trans.commit();
      ok = true;
    } // try (transaction)
    catch(const mysqlpp::BadQuery &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::raw}: " << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::raw}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::raw}: " << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::raw}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return ok;
  } // DBI::raw

  bool DBI::telemetry(aprs::APRS *aprs) {
    assert(aprs != NULL);

    Validator validator;

    std::string packet_id = aprs->getString("aprs.packet.id");
    std::string callsign_id = aprs->getString("aprs.packet.callsign.id");

    bool ok = false;
    try {
      mysqlpp::Transaction trans(*_sqlpp);
      mysqlpp::Query query = _sqlpp->query();

      //
      // query for last_telemetry
      //
      query << "INSERT INTO last_telemetry (packet_id, callsign_id, sequence, analog_0,"
            <<                             "analog_1, analog_2, analog_3, analog_4, digital, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.telemetry.sequence")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog0")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog1")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog2")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog3")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog4")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:8", aprs, "aprs.packet.telemetry.digital")
            << "," << aprs->timestamp()
            << ") ON DUPLICATE KEY UPDATE "
            << "packet_id=VALUES(packet_id), callsign_id=VALUES(callsign_id),"
            << "sequence=VALUES(sequence), analog_0=VALUES(analog_0), analog_1=VALUES(analog_1),"
            << "analog_2=VALUES(analog_2), analog_3=VALUES(analog_3), analog_4=VALUES(analog_4), digital=VALUES(digital),"
            << "create_ts=VALUES(create_ts)";
      query.execute();
      query = _sqlpp->query();

      //
      // query for telemetry
      //
      query << "INSERT INTO telemetry (packet_id, callsign_id, sequence, analog_0, analog_1,"
            <<                        "analog_2, analog_3, analog_4, digital, create_ts) VALUES "
            << "(" << mysqlpp::quote << packet_id
            << "," << callsign_id
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:int", aprs, "aprs.packet.telemetry.sequence")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog0")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog1")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog2")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog3")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "is:float", aprs, "aprs.packet.telemetry.analog4")
            << "," << mysqlpp::quote << NULL_VALID_OPTIONPP(validator, "maxlen:8", aprs, "aprs.packet.telemetry.digital")
            << "," << aprs->timestamp()
            << ")";
      query.execute();
      query = _sqlpp->query();

      trans.commit();
      ok = true;
    } // try (transaction)
    catch(const mysqlpp::BadQuery &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::telemetry}: "
                       << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::telemetry}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);

    } // catch
    catch(const mysqlpp::Exception &e) {
      STRINGTOOL_DEBUG_STRINGS(aprs, stringList, "root");
      while(!stringList.empty()) {
        TLOG(LogInfo, << "*** MySQL++ Error{Inject::telemetry}: "
                       << stringList.front() << std::endl);
        stringList.pop_front();
      } // while

      TLOG(LogWarn, << "*** MySQL++ Error{Inject::telemetry}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return ok;
  } // DBI::telemtry

  bool DBI::getIconBySymbol(const std::string &symbol_table,
                             const std::string &symbol_code,
                             const int course,
                             Icon &icon) {
    int numRows = 0;

    mysqlpp::Query query = _sqlpp->query("CALL getIconBySymbols(%0q:table, %1q:code, %2q:course)");
    query.parse();
    try {
      mysqlpp::StoreQueryResult res = query.store(symbol_table, symbol_code, course);

      for(size_t i=0; i < res.num_rows(); ++i) {
        icon.id = res[i][0].c_str();
        icon.path = res[i][1].c_str();
        icon.image = res[i][2].c_str();
        icon.icon = res[i][3].c_str();
        icon.direction = res[i][4].c_str();
      } // for

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getIconBySymbol}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getIconBySymbol}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    while(query.more_results())
      query.store_next();

    return (numRows > 0) ? true : false;
  } // DBI::getIconBySymbol

  bool DBI::getCallsignId(const std::string &source, std::string &id) {
    int numRows = 0;

    // try and get stations last message id
    //_sql->queryf("SELECT id FROM callsign WHERE source='%s' LIMIT 1",
    //            _sql->Escape(source).c_str());

    try {
      mysqlpp::Query query = _sqlpp->query("SELECT id FROM callsign WHERE source=%0q:source LIMIT 1");
      query.parse();

      mysqlpp::StoreQueryResult res = query.store(source);

      for(size_t i=0; i < res.num_rows();  i++)
        id = res[i][0].c_str();

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getCallsignId}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getCallsignId}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::getCallsignId

  bool DBI::insertCallsign(const std::string &source, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO callsign (source) VALUES ( UPPER(%0q:source) )";
      query.parse();
      res = query.execute(source);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertCallsign}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertCallsign}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;
      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertCallsign

  bool DBI::getNameId(const std::string &name, std::string &id) {
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query("SELECT id FROM object_name WHERE name=TRIM(%0q:name) LIMIT 1");
      query.parse();

      mysqlpp::StoreQueryResult res = query.store(name);

      for(size_t i=0; i < res.num_rows();  i++)
        id = res[i][0].c_str();

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getNameId}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getNameId}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::getNameId

  bool DBI::getDestId(const std::string &name, std::string &id) {
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query("SELECT id FROM destination WHERE name=%0q:name LIMIT 1");
      query.parse();

      mysqlpp::StoreQueryResult res = query.store(name);

      for(size_t i=0; i < res.num_rows();  i++)
        id = res[i][0].c_str();

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getDestId}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getDestId}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::getDestId

  bool DBI::getDigiId(const std::string &name, std::string &id) {
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query("SELECT id FROM digis WHERE name=%0q:name LIMIT 1");
      query.parse();

      mysqlpp::StoreQueryResult res = query.store(name);

      for(size_t i=0; i < res.num_rows();  i++)
        id = res[i][0].c_str();

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getDigiId}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getDigiId}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::getDigiId

  bool DBI::getMaidenheadId(const std::string &locator, std::string &id) {
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query("SELECT id FROM maidenhead WHERE locator=%0q:locator LIMIT 1");
      query.parse();

      mysqlpp::StoreQueryResult res = query.store(locator);

      for(size_t i=0; i < res.num_rows();  i++)
        id = res[i][0].c_str();

      numRows = res.num_rows();
    } // try
    catch(const mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getMaidenheadId}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
    } // catch
    catch(const mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{getMaidenheadId}: "
                    << " " << e.what()
                    << std::endl);
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::getMaidenheadId

  bool DBI::insertName(const std::string &name, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO object_name (name) VALUES ( TRIM(%0q:name) )";
      query.parse();
      res = query.execute(name);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertName}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertName}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;
      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertName

  bool DBI::insertDest(const std::string &name, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO destination (name) VALUES ( UPPER(%0q:name) )";
      query.parse();
      res = query.execute(name);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertDest}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertDest}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;
      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertDest

  bool DBI::insertDigi(const std::string &name, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO digis (name) VALUES ( UPPER(%0q:name) )";
      query.parse();
      res = query.execute(name);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertDigi}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertDigi}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;
      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertDigi

  bool DBI::insertMaidenhead(const std::string &locator, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO maidenhead (locator) VALUES ( %0q:locator )";
      query.parse();
      res = query.execute(locator);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertMaidenhead}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertMaidenhead}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;
      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertMaidenhead

  bool DBI::insertPath(const std::string &packet_id, const std::string &body) {
    mysqlpp::SimpleResult res;
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO path (packet_id, body) VALUES (%0q:packet_id, %1q:body)";
      query.parse();
      res = query.execute(packet_id, body);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPath}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPath}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::insertPath

  bool DBI::insertStatus(const std::string &packet_id, const std::string &body) {
    mysqlpp::SimpleResult res;
    int numRows = 0;

    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT IGNORE INTO statuses (packet_id, body) VALUES (%0q:packet_id, %1q:body)";
      query.parse();
      res = query.execute(packet_id, body);
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertStatus}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertStatus}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::insertStaus

  bool DBI::insertPacket(const std::string &packetId, const std::string &callsignId) {
    mysqlpp::SimpleResult res;
    int numRows = 0;

    // try and get stations last message id
    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT INTO packet (id, callsign_id, create_ts) VALUES ("
            << "UUID_TO_BIN(" << mysqlpp::quote << packetId << "),"
            << mysqlpp::quote << callsignId
            << ", UNIX_TIMESTAMP() )",
      res = query.execute();
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPacket}: #"
                    << e.errnum()
                    << " " << e.what()
                    << ": " << packetId
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPacket}: "
                    << " " << e.what()
                    << ": " << packetId
                    << std::endl);
      return false;
    } // catch

    return (numRows > 0) ? true : false;
  } // DBI::insertPacket

  bool DBI::insertPacket(const std::string &callsignId, std::string &id) {
    mysqlpp::SimpleResult res;
    int numRows = 0;
    std::stringstream s;

    s.str("");
    id = "";

    // try and get stations last message id
    try {
      mysqlpp::Query query = _sqlpp->query();
      query << "INSERT INTO packet (callsign_id, create_ts) VALUES ("
            << mysqlpp::quote << callsignId
            << ", UNIX_TIMESTAMP() )",
      res = query.execute();
      numRows = res.rows();
    } // try
    catch(mysqlpp::BadQuery &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPacket}: #"
                    << e.errnum()
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch
    catch(mysqlpp::Exception &e) {
      TLOG(LogWarn, << "*** MySQL++ Error{insertPacket}: "
                    << " " << e.what()
                    << std::endl);
      return false;
    } // catch

    if (numRows) {
      if (res.insert_id() == 0)
        return false;

      s << res.insert_id();
      id = s.str();
    } // if

    return (numRows > 0) ? true : false;
  } // DBI::insertPacket
} // namespace aprsinject
