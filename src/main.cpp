#include <cassert>
#include <exception>
#include <iostream>
#include <new>
#include <string>

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stomp/StompStats.h>

#include "App.h"

aprsinject::App *app;
openframe::Logger elog;

void usage(const char *prompt) {
  fprintf(stderr, "Usage: %s -c <config_file_path>\n", prompt);
  exit(1);
} // usage

int main(int argc, char **argv) {
  char *config = NULL;
  char *prompt = NULL;
  char ch;

  umask(077);

  // remember prompt
  if ((prompt = strrchr(argv[0], '/')))
    prompt++;
  else
    prompt = argv[0];

  // parse command line option
  bool console = false;
  while ((ch = getopt(argc, argv, "vdc:")) != -1) {
    switch (ch) {
      case 'c':
        config = optarg;
        break;
      case 'd':
        console = true;
        break;
      case 'v':
//        (void) version();
        exit(0);
        break;
      case '?':
      default:
        (void) usage(prompt);
        break;
    } // switch
  } // while

  argc -= optind;
  argv += optind;

  try {
    app = new aprsinject::App(prompt, (config) ? config : "etc/aprsinject.conf", console);
  } // try
  catch(std::bad_alloc xa) {
    assert(false);
  } // catch

  // write pid file
  if (!aprsinject::App::write_pid(aprsinject::App::kPidFile)) {
    std::cerr << "Could not write to PID file." << std::endl;
    exit(1);
  } // if

  elog.open("app", "logs/aprsinject.log");
  int loglevel = app->cfg->get_int("app.log.level", 6);
  if (loglevel >= 0 && loglevel <= 7)
    elog.level("app", static_cast<openframe::loglevel::Level>(loglevel));
  else
    elog.level("app", openframe::loglevel::LogDebug);

  app->set_elogger(&elog, "app");

  app->start();
  if (!console) {
    while( !app->is_done() ) sleep(2);
  } // if
  else app->run();
  app->stop();

  delete app;

  return 0;
} // main
