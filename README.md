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

## Tests

### Assert write to replica

Assert that a write to the master database will result the replication of the row to the remote database. This test has a forced wait in the code to give Bucardo the time to perform the replication.

### Measure duration for write to replica 

**NOTE**: This test passes but the measured duration is not correct.
