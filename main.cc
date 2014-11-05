#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <list>

#include <gflags/gflags.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#include <pqxx/pqxx>

#include <adns.h>

DEFINE_string(dbname, "", "Connect to this database");
DEFINE_string(user, "", "Connect as this user");
DEFINE_string(password, "", "Use this password to authenticate to database");
DEFINE_string(host, "", "Connect to database on this host");
DEFINE_string(port, "", "Connect to database on this port");
DEFINE_string(service, "", "Connect to database using this service");
DEFINE_string(table, "", "Modify this table");
DEFINE_string(domain, "", "Read domain from this column");
DEFINE_string(result, "", "Write response to this column");
DEFINE_int32(batchsize, 200, "Retrieve records in this batch size");
DEFINE_int32(outstanding, 5000, "Use this many concurrent DNS queries");

struct pendingquery {
  std::string domain;
  adns_query query;
};

bool exiting=false;

void sigint_handler(int)
{
  exiting = true;
}

int main(int argc, char **argv)
{
  google::SetUsageMessage("dbdnsresolve: Resolve MX records from one column of a table to another\nPerformance will be terrible unless there is an index on the column specified by --domain");
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_domain == "" || FLAGS_result == "" || FLAGS_table == "") {
    std::cerr << "All of --table, --domain and --result must be specificed\n";
    exit(1);
  }
  std::string connstring;
  if (FLAGS_dbname.find("=") != std::string::npos ||
      boost::starts_with(FLAGS_dbname, "postgres://") ||
      boost::starts_with(FLAGS_dbname, "postgresql://")) {
    connstring = FLAGS_dbname;
  } else {
    connstring="";
    if (FLAGS_dbname != "") {
      connstring += " dbname=";
      connstring += FLAGS_dbname;
    }
    if (FLAGS_host != "") {
      connstring += " host=";
      connstring += FLAGS_host;
    }
    if (FLAGS_port != "") {
      connstring += " port=";
      connstring += FLAGS_port;
    }
    if (FLAGS_user != "") {
      connstring += " user=";
      connstring += FLAGS_user;
    }
    if (FLAGS_password != "") {
      connstring += " password=";
      connstring += FLAGS_password;
    }
    if (FLAGS_service != "") {
      connstring += " service=";
      connstring += FLAGS_service;
    }
  }
  
  struct sigaction sigIntHandler;

  sigIntHandler.sa_handler = sigint_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  
  sigaction(SIGINT, &sigIntHandler, NULL);
  
  adns_state adns;
  adns_initflags initflags = adns_if_none;
  adns_init_strcfg(&adns, initflags, stderr, "nameserver 127.0.0.1");

  pqxx::connection db(connstring);
  pqxx::work t(db);

  try {
    int processed=0;
    int total;

    std::string countq = str(boost::format("select count(*) from %1% where %2% is null") % FLAGS_table % FLAGS_result);
    db.prepare("count", countq);
    pqxx::result res = t.prepared("count").exec();
    if (res.empty()) {
      std::cerr << "Failed to count table\n";
      exit(1);
    }
    total = res[0][0].as<int>();

    std::string getcq = "declare get cursor with hold for select " + FLAGS_domain + ", " + FLAGS_result + " from " + FLAGS_table + " where " + FLAGS_result + " is null order by " + FLAGS_domain;
    //    std::cerr << getcq << "\n";
    t.exec(getcq);

    std::string fetchq = str(boost::format("fetch %1% from get") % FLAGS_batchsize);
    //    std::cerr << fetchq << "\n";
    db.prepare("fetch", fetchq);

    std::string updateq = str(boost::format("update %1% set %2%=$1 where %3% = $2") % FLAGS_table % FLAGS_result % FLAGS_domain);
    db.prepare("put", updateq);

    std::list<pendingquery*> pending;

    bool reading=true;
    while(!exiting && (reading || !pending.empty())) {
      if (reading && pending.size() < (unsigned int)FLAGS_outstanding) {
        pqxx::result res = t.prepared("fetch").exec();
        if (res.empty()) {
          reading = false;
        }
        for (pqxx::result::const_iterator row = res.begin(); row != res.end(); ++row) {
          if (row[1].is_null()) {
            pendingquery *p = new pendingquery;
            p->domain = row[0].as<std::string>();
            int err = adns_submit(adns, p->domain.c_str(), adns_r_mx_raw, adns_qf_cname_loose, 0, &p->query);
            if (err) {
              std::cerr << "adns_submit failed " << strerror(err) << "\n";
              exit(1);
            }
            pending.push_back(p);
          }
        }
      }
      pendingquery *p = pending.front();
      pending.pop_front();
      adns_answer *answer;
      int err=adns_wait_poll(adns, &p->query, &answer, 0);
      if (err == EAGAIN) {
        break;
      }
      if (err) {
        std::cerr << "adns_wait_poll failed: " << strerror(err) << "\n";
        exit(1);
      }
      std::string mx = "(none)";
      if (answer->status == adns_s_ok) {
        int prio = 70000;
        adns_rr_intstr *pp = answer->rrs.intstr;
        for (int i = 0; i < answer->nrrs; ++i) {
          if (pp->i < prio) {
            mx = pp->str;
            prio = pp->i;
          }
          ++pp;
        }
      }
      t.prepared("put")(mx)(p->domain).exec();
      processed++;
      if((processed % 100) == 0) {
        std::cerr << boost::format("  %1% / %2% %|22t|%3%                    \r") % processed % total % p->domain;
      }
      //      std::cerr << p->domain << " -> " << mx << "\n";
      delete p;
    }
    t.commit();
  }
  catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    t.abort();
    exit(1);
  }
  if (exiting) {
    std::cerr << "\n\nStopping due to user request\n";
  } else {
    std::cerr << "\n\nFinished\n";
  }
  exit(0);
}
