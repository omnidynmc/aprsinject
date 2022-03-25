/**************************************************************************
 ** Dynamic Networking Solutions                                         **
 **************************************************************************
 ** OpenAPRS, Internet APRS MySQL Injector                               **
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
 **************************************************************************/

#ifndef APRSINJECT_DBI_H
#define APRSINJECT_DBI_H

#include <openframe/DBI.h>
#include <aprs/APRS.h>

namespace aprsinject {

/**************************************************************************
 ** General Defines                                                      **
 **************************************************************************/

#define NULL_OPTIONPP(x, y) ( x->getString(y).length() > 0 ? mysqlpp::SQLTypeAdapter(x->getString(y)) : mysqlpp::SQLTypeAdapter(mysqlpp::null) )
#define NULL_VALID_OPTIONPP(v, s, x, y) (( x->getString(y).length() > 0 && v.is_valid(s, x->getString(y)) )  ? mysqlpp::SQLTypeAdapter(x->getString(y)) : mysqlpp::SQLTypeAdapter(mysqlpp::null) )


/**************************************************************************
 ** Structures                                                           **
 **************************************************************************/
  struct Icon {
    std::string id;
    std::string path;
    std::string image;
    std::string icon;
    std::string direction;
  }; // struct Icon

  class DBI : public openframe::DBI {
    public:
      DBI(const openframe::LogObject::thread_id_t thread_id,
          const std::string &db,
          const std::string &host,
          const std::string &user,
          const std::string &pass);
      virtual ~DBI();

      void prepare_queries();

      bool position(aprs::APRS *aprs);
      bool message(aprs::APRS *aprs);
      bool telemetry(aprs::APRS *aprs);
      bool raw(aprs::APRS *aprs);

      bool getCallsignId(const std::string &, std::string &);
      bool getNameId(const std::string &, std::string &);
      bool getDestId(const std::string &, std::string &);
      bool getDigiId(const std::string &, std::string &);
      bool getMaidenheadId(const std::string &, std::string &);
      bool getPacketId(const std::string &);
      bool getPathId(const std::string &, std::string &);
      bool getStatusId(const std::string &, std::string &);
      bool getIconBySymbol(const std::string &symbol_table,
                           const std::string &symbol_code,
                           const int course,
                           Icon &icon);
      bool insertCallsign(const std::string &, std::string &);
      bool insertName(const std::string &, std::string &);
      bool insertDest(const std::string &, std::string &);
      bool insertDigi(const std::string &, std::string &);
      bool insertMaidenhead(const std::string &, std::string &);
      bool insertPath(const std::string &, const std::string &);
      bool insertPacket(const std::string &, const std::string &);
      bool insertPacket(const std::string &, std::string &);
      bool insertStatus(const std::string &, const std::string &);

    protected:
    private:
  }; // class DBI

/**************************************************************************
 ** Macro's                                                              **
 **************************************************************************/

/**************************************************************************
 ** Proto types                                                          **
 **************************************************************************/

} // namespace aprsinject
#endif
