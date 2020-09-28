# crux-xodus

[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg)](https://clojars.org/avisi-apps/crux-xodus)

[Crux](https://github.com/juxt/crux) Currently supports LMDB and RocksDB, but we wanted a pure Java solution so we created a [Xodus](https://github.com/JetBrains/xodus)-backed KV store.

# Usage

If you want to quickly try it out you should follow the [Official Crux installation](https://opencrux.com/reference/).

## Add a dependency
Make sure to first add this module as a dependency: 
[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg)](https://clojars.org/avisi-apps/crux-xodus)


## Configure Crux
And after that you can change the KV backend to the Xodus one:

**EDN**
```clojure
{:crux/index-store {:kv-store {:crux/module 'avisi.crux.xodus/->kv-store
                               :db-dir (io/file "/tmp/xodus")}}
 :crux/document-store {...}
 :crux/tx-log {...}}
```

**JSON**
```json
{
  "crux/index-store": {
    "kv-store": {
      "crux/module": "avisi.crux.xodus/->kv-store",
      "db-dir": "/tmp/xodus"
    }
  },
 
  "crux/document-store": { ... },
  "crux/tx-log": { ... }
}
```
For more information about configuring Crux see: https://opencrux.com/reference/configuration.html
