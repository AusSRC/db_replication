# Database replication and distribution of data products

This repository is part of a research work on the feasibility of using data replication tools and services between different geographically separated sites. In this context a poster has been developed to highlight the use of such tools to solve the problem of data distribution and replication.

In this repository you will find all the necessary material for:
- Reproduce all the experimentation of the tests carried out with the Bucardo replication tool:
    - Set-up, development and deployment.
    - Benchmarks generation.
- Validate the results obtained.
- Reproduce the results, data, and diagrams using a NoteBook with JupuyterHub.

Poster: 
" Asymmetric distribution of data products from WALLABY, an SKA precursor neutral hydrogen survey" 

## Create a Bucardo environment

In order to create an initial environment you have to install:

- PostgreSQL
- Bucardo ()

After that, an extra database schema is required, to do that follow the [next tutorial](./docs/bucardo_binary.md).


A collection of Python code for a database replication helper class and tests. 

## Running test locally

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
The *time difference* provides the total delay time in processing the whole replication for this concrete dbsync as indicated in our Bucardo setup.

### Light tables benchmark

```
cd src
python -m unittest tests/bucardo-benchmarking-lighttables.py -vvv
```



### Fat tables benchmark

In order to start working with heavy tables benchmark, you need to set up a test table to enable our metrics. This new table will be used to store binary (bytea type) data products generated randomly, avoiding to change original Wallaby databases and tables.

To create this environment, before start the test, please follow the [next tutorial](./docs/bucardo_binary.md).

Once created the test environment, start the tests

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
2021-09-20 11:31:48.070508+00:00,2021-09-20 11:31:52.505424+00:00,200,delete,wallaby.test_fattable
2021-09-20 11:31:54.017121+00:00,2021-09-20 11:31:59.288187+00:00,1000,insert,wallaby.run
2021-09-20 11:32:03.258886+00:00,2021-09-20 11:32:07.721145+00:00,1000,delete,wallaby.run
```

CVS schema is: ``start_date``,``end_date``,``rows``,``operation``,``table``

```
cat results.csv
```

Each execution will append data to this results file.

Another way to extract metrics is to export results from ``syncrun`` table in Bucardo:

```
Copy (select inserts, deletes, started, ended, status, details from syncrun order by started DESC) To '/tmp/test.csv' With CSV DELIMITER ',' HEADER;
```

That will be produce the following data:

```
inserts,deletes,started,ended,status,details
0,10,2021-09-27 09:37:21.339024+00,2021-09-27 09:37:26.835931+00,Complete (KID 1706),""
5,0,2021-09-27 09:35:30.097892+00,2021-09-27 09:36:02.355176+00,Complete (KID 1706),""
0,10,2021-09-27 09:33:35.265159+00,2021-09-27 09:33:40.096236+00,Complete (KID 1706),""
5,0,2021-09-27 09:31:44.0273+00,2021-09-27 09:32:15.78222+00,Complete (KID 1706),""
```


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
        ["insert/delete", <rows>, <size>],
        ["insert/delete", <rows>, <size>],
        ["insert/delete", <rows>, <size>],
        ...
    ])
````

Where  `insert/delete` is the operation performed within the unit tests, ``<rows>`` is the number os rows to insert/delete at the same time, and ``<size>`` is the binary data size of the object that will be replicated (for heavy tables).


## Results and reproducibility

To reproduce plots and get the data ready to extract statistics, we encorage to use a JupyterLab Notebook:
[results plots and diagrams](./results/benchmark_results.ipynb).

Also you can use 
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/manuparra/db_replication/HEAD?urlpath=https%3A%2F%2Fgithub.com%2Fmanuparra%2Fdb_replication%2Fblob%2Fmain%2Fresults%2Fbenchmark_results.ipynb)

# References

(1) Koribalski, Bärbel S., Lister Staveley-Smith, Tobias Westmeier, Paolo Serra, Kristine Spekkens, O. I. Wong, Karen Lee-Waddell et al. "WALLABY–an SKA Pathfinder H i survey." Astrophysics and Space Science 365, no. 7 (2020): 1-35.
(2) Thomas, Shaun M. PostgreSQL High Availability Cookbook. Packt Publishing Ltd, 2017.
(3)Nakanishi,H.,K.Yamanaka,S.Tokunaga,T.Ozeki,Y.Homma,H.Ohtsu,Y.Ishiietal."Designforthedistributeddatalocatorservicefor multi-sitedatarepositories."FusionEngineeringandDesign165(2021):112197.
(4) PostgreSQL. “Replication, Clustering, and Connection Pooling.” Replication, Clustering, and Connection Pooling - PostgreSQL wiki. Accessed October 14, 2021. https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling. (5) Github - Aussrc/Db_Replication: Multi-Master Database Replication Experiment". 2021. Github. https://github.com/AusSRC/db_replication.


# Acknowledgements

We acknowledge financial support from the State Agency for Research of the Spanish Ministry of Science, Innovation and Universities through the "Center of Excellence Severo Ochoa" awarded to the Instituto de Astrofísica de Andalucía (SEV-2017-0709) and from the grant RTI2018-096228-B-C31 (Ministry of Science, Innovation and Universities / State Agency for Research / European Regional Development Funds, European Union). In addition, we acknowledge financial support from the Ministry of Science, Innovation and Universities and the European Regional Development Funds (EQC2019-005707-P) and the Regional Government of Andalusia (SOMM17-5208- IAA-2017). We also acknowledge financial support from the Ministry of Science and Innovation, from the budgetary line 28.06.000x.430.09 of the General State Budgets of 2021, for the coordination of the participation in SKA- SPAIN and finally we acknowledge financial support from the grant 54A Scientific Research and Innovation Program (Regional Council of Economy, Knowledge, Business and Universities, Regional Government of Andalusia and the European Regional Development Funds 2014-2020, program D1113102E3).
