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
 $Id: APRS.cpp,v 1.2 2005/12/13 21:07:03 omni Exp $
 **************************************************************************/

#include <list>
#include <new>
#include <string>
#include <cstdarg>
#include <cstdio>
#include <cassert>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>

#include <dlfcn.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include <openframe/openframe.h>

#include "UnitTest.h"
#include "Validator.h"

namespace aprsinject {

/**************************************************************************
 ** UnitTest Class                                                       **
 **************************************************************************/

/******************************
 ** Constructor / Destructor **
 ******************************/

  UnitTest::UnitTest() : _ok(true) {
    // set defaults
  } // UnitTest::UnitTest

  UnitTest::~UnitTest() {
    // cleanup
  } // UnitTest::~UnitTest

  /***************
   ** Variables **
   ***************/

  const bool UnitTest::run() {
    _length();

    return ok();
  } // UnitTest::run

  void UnitTest::_length() {
    _test("maxlen:5", "this", "max length valid", true);
    _test("maxlen:t", "this", "max length bad field", false, true);
    _test("minlen:5", "thisisme", "min length valid, success", true);
    _test("minlen:t", "this", "max length bad field", false, true);
    _test("minlen:5", "this", "min length invalid", false);
    _test("minlen:5|maxlen:10", "this", "min/max length invalid", false);
    _test("minlen:5|maxlen:10", "thisisfun", "min/max length valid", true);
    _test("minlen:5|maxlen:10", "thisisfunyeah", "min/max length invalid long", false);
    _test("chrng:97-100", "abcd", "character range valid a-d (97-100)", true);
    _test("chrng:97-100", "abcde", "character range invalid a-e (97-101)", false);
    _test("chrng:97-100", "`abcd", "character range invalid `-d (96-100)", false);
    _test("chrng:101-96", "abcd", "character range bad range start greater than end", false, true);
    _test("chrng:101-", "abcd", "character range bad range missing end", false, true);
    _test("chrng:-100", "abcd", "character range bad range missing start", false, true);
    _test("chrng:-", "abcd", "character range bad range dash only", false, true);
    _test("chrng:string", "abcd", "character range bad range no numerics", false, true);
    _test("chrng:str-stg", "abcd", "character range bad range no numerics", false, true);
    _test("chrng:100-stg", "abcd", "character range bad range end string", false, true);
    _test("chrng:str-100", "abcd", "character range bad range begin string", false, true);
    _test("chpool:abcdefg", "abcd", "character pool valid", true);
    _test("chpool:abcdefg", "abcdz", "character pool invalid", false);
    _test("is:float", "abcdz", "is float invalid", false);
    _test("is:float", "1.20", "is float valid", true);
    _test("is:float", "-1.20", "is float valid", true);
    _test("is:float", "-.20", "is float valid", true);
    _test("is:float", ".20", "is float valid", true);
    _test("is:int", "1.20", "is int invalid", false);
    _test("is:int", "120", "is int valid", true);
    _test("is:int", "1", "is int valid", true);
    _test("is:int", "1abs", "is int invalid", false);
    _test("minval:10", "100", "min value valid", true);
    _test("minval:9", "10", "min value valid", true);
    _test("minval:12", "10", "min value invalid", false);
    _test("minval:12", "abcd", "min value invalid not a number", false);
    _test("minval:-12", "2", "min value valid negative number", true);
    _test("minval:-12", "-20", "min value valid negative number", false);
    _test("minval:-12", "-10", "min value valid negative number", true);
    _test("minval:abs", "abcd", "min value bad vars string", false, true);
    _test("maxval:10", "100", "max value invalid", false);
    _test("maxval:10", "9", "max value invalid", true);
    _test("maxval:10", "-9", "max value valid", true);
    _test("maxval:-9", "-10", "max value valid", true);
    _test("maxval:-9", "-8", "max value valid", false);
    _test("maxval:abs", "abcd", "max value bad vars string", false, true);
  } // UnitTest::_length

  const bool UnitTest::_test(const std::string &vars, const std::string &str, const std::string &testName, const bool expect, const bool exception) {
    bool isValid = true;
    Validator v = vars;

    try {
      isValid = v.is_valid(vars, str);
    } // try
    catch(Validator_Exception &e) {
      if (exception) {
        std::cout << " ok - " << testName << "; expected exception caught: '" << e.message() << "'" << std::endl;
        ok(true);
        return true;
      } // if

      std::cout << " not ok - " << testName << " failed to validate: " << testName << "; " << e.message() << std::endl;
      ok(false);
      return false;
    } // catch

    if (exception == true) {
      // we expected an exception but didn't catch it
      std::cout << " not ok - " << testName << " failed to meet expectations: " << testName << "; expected exception" << isValid << std::endl;
    } // if

    if (isValid != expect) {
      std::cout << " not ok - " << testName << " failed to meet expectations: " << testName << "; expected " <<  expect << " got " << isValid << std::endl;
      ok(false);
      return false;
    } // if

    std::cout << " ok - " << testName << std::endl;

    return true;
  } // UnitTest::_test

} // namespace aprsinject
