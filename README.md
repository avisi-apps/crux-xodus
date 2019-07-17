# crux-xodus

[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg)](https://clojars.org/avisi-apps/crux-xodus)

[Crux](https://github.com/juxt/crux) Currently supports LMDB and RocksDB and we wanted a pure java Solution to we created [Xodus](https://github.com/JetBrains/xodus) backed KV store.

# Usage

If you want to quickly try it out you should follow the [Official Crux tutorial](https://juxt.pro/crux/docs/configuration.html#standalone).
And after that you can change the KV backend to the Xodus one:

```clojure
{:kv-backend "avisi.crux.kv.xodus.XodusKv"
 :db-dir "data/db-dir-1"
 :event-log-dir "data/eventlog-1"}
```
