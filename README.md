# crux-xodus

Crux has been renamed to XTDB.

[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg)](https://clojars.org/avisi-apps/crux-xodus)

[XTDB](https://github.com/xtdb/xtdb) Currently supports LMDB and RocksDB, but we wanted a pure Java solution so we created a [Xodus](https://github.com/JetBrains/xodus)-backed KV store.

# Usage

If you want to quickly try it out you should follow the [Official XTDB installation](https://xtdb.com/docs/).

## Add a dependency
Make sure to first add this module as a dependency: 
[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg)](https://clojars.org/avisi-apps/crux-xodus)


## Configure XTDB
And after that you can change the KV backend to the Xodus one:

**EDN**
```clojure
{:xtdb/index-store {:kv-store {:xtdb/module 'avisi.crux.xodus/->kv-store
                               :db-dir (io/file "/tmp/xodus")}}
 :xtdb/document-store {...}
 :xtdb/tx-log {...}}
```

**JSON**
```json
{
  "xtdb/index-store": {
    "kv-store": {
      "xtdb/module": "avisi.crux.xodus/->kv-store",
      "db-dir": "/tmp/xodus"
    }
  },

  "xtdb/document-store": { ... },
  "xtdb/tx-log": { ... }
}
```
For more information about configuring XTDB see: https://docs.xtdb.com/administration/configuring/

# Developer

## Releasing

First make sure the pom is up-to-date run
```
$ clojure -Spom
```

Edit the pom to have the wanted version and commit these changes.
Create a tag for the version for example:

```
$ git tag <your-version>
$ git publish origin <your-version>
```

Make sure you have everything setup to release to clojars by checking these [instructions](https://github.com/clojars/clojars-web/wiki/Pushing#maven).
After this is al done you can release by running:

```
$ mvn deploy
```
