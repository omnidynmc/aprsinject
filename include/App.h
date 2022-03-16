#ifndef APRSINJECT_APP_H
#define APRSINJECT_APP_H

#include <string>
#include <deque>

#include <openframe/openframe.h>
#include <openframe/App/Application.h>
#include <stomp/StompStats.h>

namespace aprsinject {
/**************************************************************************
 ** General Defines                                                      **
 **************************************************************************/

/**************************************************************************
 ** Structures                                                           **
 **************************************************************************/
  class App : public openframe::App::Application {
    public:
      typedef openframe::App::Application super;

      typedef std::deque<pthread_t> workers_t;
      typedef workers_t::iterator workers_itr;
      typedef workers_t::const_iterator workers_citr;
      typedef workers_t::size_type workers_st;

      static const char *kPidFile;

      App(const std::string &prompt, const std::string &config, const bool console=false);
      virtual ~App();

      void onInitializeSystem();
      void onInitializeConfig();
      void onInitializeCommands();
      void onInitializeDatabase();
      void onInitializeModules();
      void onInitializeThreads();

      void onDeinitializeSystem();
      void onDeinitializeCommands();
      void onDeinitializeDatabase();
      void onDeinitializeModules();
      void onDeinitializeThreads();

      void rcvSighup();
      void rcvSigusr1();
      void rcvSigusr2();
      void rcvSigint();
      void rcvSigpipe();

      bool onRun();

      static void *WorkerThread(void *arg);

      stomp::StompStats *stats() { return _stats; }

    protected:
    private:
      workers_t _workers;
      stomp::StompStats *_stats;
  }; // App

/**************************************************************************
 ** Macro's                                                              **
 **************************************************************************/

/**************************************************************************
 ** Proto types                                                          **
 **************************************************************************/
} // namespace openstomp

extern aprsinject::App *app;
extern openframe::Logger elog;
#endif
