#ifndef APRSINJECT_WORKER_H
#define APRSINJECT_WORKER_H

#include <string>
#include <vector>
#include <list>

#include <openframe/openframe.h>

namespace aprsinject {
/**************************************************************************
 ** General Defines                                                      **
 **************************************************************************/

/**************************************************************************
 ** Structures                                                           **
 **************************************************************************/

  class Validator_Exception : public openframe::OpenFrame_Exception {
    public:
      Validator_Exception(const std::string message) throw() : openframe::OpenFrame_Exception(message) { };
  }; // class Validator_Exception

  class Validator {
    public:
      // ### Constants ### //
      static const std::string kIsFloatName;
      static const std::string kIsIntName;
      static const std::string kIsName;
      static const std::string kChrngName;
      static const std::string kChpoolName;
      static const std::string kMinLenName;
      static const std::string kMaxLenName;
      static const std::string kMinValName;
      static const std::string kMaxValName;

      //static const char *kFloatName;
      //static const char *kIntName;

      // ### Init ### //
      Validator();
      Validator(const std::string);
      virtual ~Validator();

      // ### Type Definitions ###
      //typedef locators_t::size_type locators_st;

      // ### Options ### //

      void set_vars(const std::string);
      const bool is_valid(const std::string);
      const bool is_valid(const std::string, const std::string);

    protected:
      const bool is_character_range(const std::string &, const int, const int);
      const bool is_character_pool(const std::string &, const char *);
      const bool is_max_len(const std::string &, const std::string::size_type);
      const bool is_min_len(const std::string &, const std::string::size_type);
      const bool is_max_val(const std::string &, const int);
      const bool is_min_val(const std::string &, const int);
      const bool is_float(const std::string &);
      const bool is_int(const std::string &);

    private:
      // constructor variables
      openframe::Vars _vars;

  }; // class Validator

/**************************************************************************
 ** Macro's                                                              **
 **************************************************************************/

/**************************************************************************
 ** Proto types                                                          **
 **************************************************************************/
} // namespace aprsinject
#endif
