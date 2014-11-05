=== dbdnsresolve

This is a quick and fairly dirty high performance DNS resolution tool,
designed for quick overview MX surveying.

It reads a table in a postgresql database that has at least two columns, one
containing domain names and the other a result column initially containing
NULLs. For each domain name it resolves the MX record, then writes one of the
highest-priority MXes into the result column.

It will run many DNS resolutions in parallel (5000) by default, and will put
a high load on the recursive resolver it uses. It's intended to use a
dedicated recursive resolver, so is currently hardwired to send queries
via 127.0.0.1.

`dbdnsresolve --help` will display all the supported commandline flags.

`dbdnsresolve --table=<tablename> --domain=<column> --result=<column>` is
the minimum require set of options.

Build with "make", install to /usr/local/bin with "sudo make install".

Prerequisites are [adns](http://www.gnu.org/software/adns/),
[libpqxx](http://pqxx.org/development/libpqxx/) version 4 or later,
[gflags](https://code.google.com/p/gflags/) and a reasonably recent
[boost](http://www.boost.org) installation.
