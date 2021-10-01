# Bucardo setup for heavy tables benchmark

In order to be able to run the tests on heavy tables for Wallaby data products, that include bytea type data in postgresql, it is necessary to create the following structure in Bucardo.

To do so:


- Create the following table in all locations:

```
 id     | integer                |           | not null | nextval('wallaby.test_fattable_id_seq'::regclass)
 name   | character varying(255) |           |          | 
 data   | bytea                  |           |          | 
```

```
 CREATE TABLE wallaby.test_fattable (
  id SERIAL ,
  name VARCHAR(255),
  data bytea
 );
```


(previously you must have created the [Wallaby schema](https://github.com/AusSRC/WALLABY_database)).

- We will add a new data distribution sync in one of the locations that are data source (only in one, for unidirectional tests to multiple locations).

```
  bucardo add db benchmark dbname=sofiadb user=bucardo port=18020
  bucardo add db benchmark_cirada dbname=sofiadb user=bucardo host=206.12.93.99
  bucardo add db benchmark_aussrc dbname=sofiadb user=bucardo host=146.118.69.200
```

- Add a relgroup:

```
 bucardo add relgroup benchmark_relgroup wallaby.test_fattable
```

- Add a dbgroup:

```
 bucardo add dbgroup benchmark_dbgroup benchmark:source benchmark_aussrc:target benchmark_cirada:target
```

- Add a new sync for Benchmark tables:

```
  bucardo add sync benchmark_dbsync relgroup=benchmark_relgroup dbgroup=benchmark_dbgroup
```

- Restart bucardo to start the replication service for the last sync created

```
 bucardo restart "Added Benchmark Suite"
```
