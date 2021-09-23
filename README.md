# Database replication

A collection of Python code for a database replication helper class and tests. 

## Running locally

We will go through how to run the test found at `src/tests/one-directional.py`. Note that some default database credentials are provided, but to run the test you will need to provide a valid password for the local and remote PostgreSQL instances.

First set up the virtual environment and install the dependencies 

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Then you will be able to run the test case from inside the `src` directory

```
cd src 
python -m unittest tests/one-directional.py
```

## Tests and Benchmarks from Bucardo

To carry out these tests and benchmarks, the use of ad-hoc triggers to audit the state of the tables in the different locations was initially discarded as it produced cycles. Thus Bucardo internally disables the triggers in the target locations. 

To solve this problem we have used the information that Bucardo generates inside the Bucardo.syncrun table. 

This table controls:
- The operation or group of operations (if they are grouped by transaction ID) 
- the start time, 
- the end time, and the 
- number of registers for the operations.

The *start time* corresponds to when the records are inserted in the source table of the location where the query is executed.
The *end time* corresponds to the time at which all locations have consolidated the data. 
The *time difference* provides the total delay time in processing the whole replication for this concrete dbsync as indicated in out Bucardo setup.

### Light tables benchmark

```
cd src
python -m unittest tests/bucardo-benchmarking-lighttables.py -vvv
```

### Fat tables benchmark

```
cd src
python -m unittest tests/bucardo-benchmarking-fattables.py -vvv
```

### Foreign key contrainted tables benchmark

```
cd src
python -m unittest tests/bucardo-benchmarking-cascade.py -vvv
```

## Results


After that in the ``results.csv`` file will be the results of the benchmarks, with the following format (example):

```
2021-09-20 11:31:48.070508+00:00,2021-09-20 11:31:52.505424+00:00,200,delete
2021-09-20 11:31:54.017121+00:00,2021-09-20 11:31:59.288187+00:00,1000,insert
2021-09-20 11:32:03.258886+00:00,2021-09-20 11:32:07.721145+00:00,1000,delete
```

CVS schema is: ``start_date``,``end_date``,``rows``,``operation``

```
cat results.csv
```

Each execution will append data to this results file.

## Templating tables for the benchmarks

The set of tables templates that can be used from the unit tests and the benchmark are in ``src`` folder with the name ``<tablename>_<operation>.template``. These tables contain a template to autogenerate records in batch for different cases, for example insert or update in a single statement.

Example of ``wallaby.run`` (insert):

```
INSERT INTO  wallaby.run(id, name, sanity_thresholds) VALUES
({nrow},'benchmark_{nrow}', '{"flux": 10, "spatial_extent": [10, 10], "spectral_extent": [10, 10], "uncertainty_sigma": 5}')
```

In this case ``{nrow}`` will be overwritten to generate a set on rows with the second line (first line, just one time). This template can be overloaded to include new variables (i.e. random contend for ``sanity_thresholds``).

Example template for ``wallby.run`` (delete):

```
DELETE FROM wallaby.run where name LIKE '{name}_%';
```

Here ``{name}`` will be changed by the name of the bechmark inserted rows with this ``name``.

## Meta-testing

Each generated sequence of data for the tables can be checked in the ``src`` directory with the form ``<rows>_<operation>.sql``

## Customizing

There are several options to customize the benchmarks:

- Results file: change the value of results.csv in X.

```
OUTPUTFILE = "results.csv"
```

- Sequence start ID: 

```
STARTING_ID = 50000
```

- Parameterization of the benchmarks: to do this in the decorator structure used here, you can include the set of operations and records to test, and it will affect to function test you decorate it.

````
@parameterized.expand([
        ["insert/delete", 100],
        ["insert/delete", 200],
        ["insert/delete",1000],
    ])
````




