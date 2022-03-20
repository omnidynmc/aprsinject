#include "config.h"

#include <string>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <openframe/openframe.h>

#include "Validator.h"

namespace aprsinject {
  const std::string Validator::kIsFloatName = "float";
  const std::string Validator::kIsIntName = "int";
  const std::string Validator::kIsName = "is";
  const std::string Validator::kChrngName = "chrng";
  const std::string Validator::kChpoolName = "chpool";
  const std::string Validator::kMinLenName = "minlen";
  const std::string Validator::kMaxLenName = "maxlen";
  const std::string Validator::kMinValName = "minval";
  const std::string Validator::kMaxValName = "maxval";

  Validator::Validator() {
  } // Validator::Validator

  Validator::Validator(const std::string vars) : _vars(vars) {
  } // Validator::Validator

  Validator::~Validator() {
  } // Validator::~Validator

  void Validator::set_vars(const std::string vars) {
    _vars = vars;
  } // Validator

  // Valid Arguments
  // chrng  : <int>-<int>
  // chpool : <characters>
  // maxval : <int>
  // minval : <int>
  // maxlen : <int>
  // minlen : <int>
  // is     : <float|int>
  const bool Validator::is_valid(const std::string vars, const std::string str) {
    _vars = vars;
    return is_valid(str);
  } // Validator::is_valid

  const bool Validator::is_valid(const std::string str) {
    // --- IS <float|int> ===
    if (_vars.is(Validator::kIsName) == true) {
      std::string is = _vars.get(Validator::kIsName);

      if (is == Validator::kIsFloatName) return is_float(str);
      if (is == Validator::kIsIntName) return is_int(str);
    } // if

    bool is_valid = true;
    // --- MINLEN ===
    if (_vars.is(Validator::kMinLenName)) {
      std::string minLen = _vars.get(kMinLenName);
      if (is_int(minLen) == false) throw Validator_Exception("is_valid: minLen: min value not a number");
      std::string::size_type len = (std::string::size_type) atoi(minLen.c_str());
      is_valid &= is_min_len(str, len);
    } // if

    // --- MAXLEN ===
    if (_vars.is(Validator::kMaxLenName)) {
      std::string maxLen = _vars.get(kMaxLenName);
      if (is_int(maxLen) == false) throw Validator_Exception("is_valid: maxLen: max value not a number");
      std::string::size_type len = (std::string::size_type) atoi(maxLen.c_str());
      is_valid &= is_max_len(str, len);
    } // if

    // --- MINVAL ===
    if (_vars.is(Validator::kMinValName)) {
      std::string minVal = _vars.get(kMinValName);
      if (is_int(minVal) == false) throw Validator_Exception("is_valid: minVal: min value not a number");
      int val = atoi(minVal.c_str());
      is_valid &= is_min_val(str, val);
    } // if

    // --- MAXVAL ===
    if (_vars.is(Validator::kMaxValName)) {
      std::string maxVal = _vars.get(kMaxValName);
      if (is_int(maxVal) == false) throw Validator_Exception("is_valid: maxVal: max value not a number");
      int val = atoi(maxVal.c_str());
      is_valid &= is_max_val(str, val);
    } // if

    // --- CHRNG ---
    if (_vars.is(Validator::kChrngName)) {
      std::string chrng = _vars.get(kChrngName);

      std::string::size_type pos = chrng.find('-');
      if (pos == std::string::npos) throw Validator_Exception("is_valid: chrng: invalid format, should be <int>-<int>");

      std::string minStr = chrng.substr(0, pos);
      if (minStr.length() == 0 || minStr.find_first_not_of("0123456789") != std::string::npos)
        throw Validator_Exception("is_valid: chrng: invalid format, missing min value"); 

      int min = atoi(minStr.c_str());

      if (++pos == chrng.length()) throw Validator_Exception("is_valid: chrng: invalid format, missing max value");

      std::string maxStr = chrng.substr(pos, chrng.length());
      if (maxStr.length() == 0 || maxStr.find_first_not_of("0123456789") != std::string::npos)
        throw Validator_Exception("is_valid: chrng: invalid format, missing min value"); 

      int max = atoi(maxStr.c_str());

      is_valid &= is_character_range(str, min, max);
    } // if

    // --- CHPOOL ---
    if (_vars.is(Validator::kChpoolName)) {
      std::string chpool = _vars.get(kChpoolName);
      is_valid &= str.find_first_not_of(chpool) == std::string::npos;
    } // if

    return is_valid;
  } // Validator::is_valid

  /***************
   ** Protected **
   ***************/

  const bool Validator::is_int(const std::string &str) {
    if (str.empty() || ((!isdigit(str[0])) && (str[0] != '-'))) return false;

    if (str[0] == '-' && str.length() > 1) {
      std::string str2 = str.substr(1, str.length());
      return str2.find_first_not_of( "0123456789" ) == std::string::npos;
    } // if

    return str.find_first_not_of( "0123456789" ) == std::string::npos;
  } // Validator::is_int

  const bool Validator::is_float(const std::string &str) {

    std::string::const_iterator it = str.begin();

    bool decimalPoint = false;
    std::string::size_type minSize = 0;

    if (str.size() > 0 && (str[0] == '-' || str[0] == '+')) {
      it++;
      minSize++;
    } // if

    while(it != str.end()) {
      if (*it == '.') {
        if (!decimalPoint) decimalPoint = true;
        else break;
      }
      else if (!std::isdigit(*it) && ((*it!='f') || it+1 != str.end() || !decimalPoint)) {
        break;
      }
      ++it;
    }
    return str.size() > minSize && it == str.end();
  } // Validator::is_float

  const bool Validator::is_min_val(const std::string &str, const int min_val) {
    if (!is_int(str)) return false;
    int val = atoi(str.c_str());

    return val > min_val;
  } // Validator::is_min_val

  const bool Validator::is_max_val(const std::string &str, const int max_val) {
    if (!is_int(str)) return false;
    int val = atoi(str.c_str());

    return val < max_val;
  } // Validator::is_max_val

  const bool Validator::is_min_len(const std::string &str, const std::string::size_type len) {
    return str.length() > len;
  } // Validator::is_min_len

  const bool Validator::is_max_len(const std::string &str, const std::string::size_type len) {
    return str.length() < len;
  } // Validator::is_max_len

  const bool Validator::is_character_range(const std::string &str, const int min, const int max) {
    if (min >=max) {
      throw Validator_Exception("is_character_range: min must be less than max");
    } // if

    std::string::const_iterator it = str.begin();
    while(it != str.end()) {
      const char c = *it;
      if (c >= min && c <= max) {
        ++it;
        continue;
      }

      return false;
    } // while

    return true;
  } // Validator::is_character_range

  const bool Validator::is_character_pool(const std::string &str, const char *characters) {
    return str.find_first_not_of(characters) == std::string::npos;
  } // Validator::is_character_pool


} // namespace aprsinject
