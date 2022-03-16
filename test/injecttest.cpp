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

#include <openframe/openframe.h>
#include <stomp/Stomp.h>
#include <stomp/StompFrame.h>
#include <stomp/StompClient.h>
#include <stomp/StompHeader.h>


#define INJECT "WB4APR-3>APRX76,WIDE2-2,qAR,N3UJJ:;DF3      *021736z350 .  N/0752 .  W\\001/000/055/968/DFreport"
#define QUEUE "/queue/feeds.aprs.is"

int main(int argc, char **argv) {

  if (argc < 2) {
    std::cerr << "ERROR missing host argument" << std::endl;
    exit(1);
  } // if

  std::string hosts = argv[1];

  stomp::Stomp *stomp;
  try {
    stomp = new stomp::Stomp(hosts, "user", "pass");
  } // try
  catch(stomp::Stomp_Exception ex) {
    std::cout << "ERROR: " << ex.message() << std::endl;
    return 0;
  } // catch

  while(1) {
    std::cout << "Connecting to stomp server" << std::endl;

    bool ok = stomp->subscribe("/queue/feeds.*", "1");
    if (!ok) {
      std::cout << "not connected, retry in 2 seconds" << std::endl;
      sleep(2);
      continue;
    } // if

    std::stringstream s;
    s << time(NULL)
      << " "
      << INJECT
      << std::endl;
std::cout << s.str() << std::endl;
    stomp::StompFrame *frame = new stomp::StompFrame("SEND", s.str());
    frame->add_header("destination", QUEUE);
    stomp->send_frame(frame);
    frame->release();

    break;
  } // while

  return 0;
} // main
