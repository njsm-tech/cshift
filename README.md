This is a framework for detecting covariate shift (ie, distributional shift) in features for machine learning pipelines. It allows the user to compare two datasets, using different comparison methods, to see if the datasets are distributionally the same. 

The major abstractions are: 
- datasets, which are composed of columns
- models
- comparisons, representing algorithms for comparing two datasets to test if they are distributionally the same
- results and result sets

Datasets can be given as pandas dataframes for in-memory processing. The library is structured as a client-service architecture, so that comparisons can be run remotely on large datasets. 

To run tests, run `./tests.sh` from within the `tests` folder.
