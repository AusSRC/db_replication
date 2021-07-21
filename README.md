# Database replication

A collection of Python code for a database replication helper class and tests.

## Tests

### Assert write to replica

Assert that a write to the master database will result the replication of the row to the remote database. This test has a forced wait in the code to give Bucardo the time to perform the replication.

### Measure duration for write to replica 

**NOTE**: This test passes but the measured duration is not correct.
