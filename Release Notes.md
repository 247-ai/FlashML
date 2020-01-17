# Release Notes

## FlashML Release 2019.3

### User-centric optimizations & Innovations

* Support for validating config file submitted by the user
* Final prediction for multi-class SVM models is now derived from the Platt scaling step.
* Platt scaling is now optional while running an SVM workflow
* Optimization of caching for speedier executions
* Created journey datasets for running tests which can be open sourced

### Bug Fixes
* The calculations for generating confusion metrics was incorrect. This has been fixed.
* Unsupported preprocessing transformations now generates a custom error in Publish
* Bug fixes in JS generation
* Other minor bug fixes

## FlashML Release 2019.1

### Features
Spark Update

    FlashML Spark modules have been upgraded to the version 2.4.1
    Saving selected columns of df after cross-validation 

Bug Fixes - FlashML

    A bug fix for a model with zero coefficients. It was due to higher positive class percent mentioned in the config file. The positive class should be given less than 50%
    Timeout exception in cluster mode is resolved by making the context as "Yarn" and also an exception has been added if it is not in this combination
    Filtering the df by pages before training leads to ArrayIndexOutOfBound exception which has been fixed now

User-centric optimizations & Innovations

    Regex functions like removal and replacement are now supported
    FlashML supports the declaration of variables and config. parameters on a 'scope' approach, making it easier to build multi-level page models
    Bug fixes to QA Data Generation
    Publish JS scripts have been optimized for execution

## FlashML Release 2018.4
### Features
Spark Update

    FlashML Spark modules have been upgraded to the version 2.4.0.

User-centric optimizations & Innovations

    We now support publishing trained NL models in Web2NL format
    We now support binning of numeric variables. The specific column can be binned using equi-distance, equi-area or user-defined buckets. The corresponding JS can be used in a prediction server.
    We now support null handling and imputation for every column. The corresponding JS can be used in a prediction server.
    Updated Platt Scaling for SVM models for calculating probability as per research paper.
    Added support for running Cross-Validation for hyper-parameter optimization for the supported models, including Platt Scaling support for SVM models.
    Complete log of training jobs can be monitored real-time using Kibana
    Detailed metrics are now printed for CV folds. Prediction scores from the CV experiment are saved in HDFS for further analysis.
    Some updates to input JSON file for FlashML
    Bug fixes to published JS codes
    
## FlashML Release 2018.3.1
### Bug Fixes
Javascript Fix:

    In tokenizer, the regex pattern was not created properly. Some of the special pattern in not escaped properly. That has been fixed.
    While checking the page count in javascript, the equality operations are not performed in a correct manner which is fixed now.

### Features:

    The prediction column was indexed before which was very difficult for comparison. Now a new column has been created which has predicted intent names rather than numbers.
    In cross validation the metrics were computed based on the actual dataset rather than a fold level datasets. Now the metrics are computed based on that.
    
## FlashML Release 2018.3
### Features
Spark Update

    FlashML Spark modules have been upgraded to the version 2.3.1.

User-centric optimizations & Innovations

    Optimized the computation of custom metrics for models on journey data, resulted in 300% improvement in computation time
    We now support centralized logging from FlashML application to an ELK server. This can be used for searching and monitoring a FlashML job.
    We now support configuration file in JSON format.
        The configuration file can be partially provided: the rest of the fields would be auto-populated from a default configuration file.
    For NL models, the Regex-based replacement transformations are now separated out from word-class replacements. This results in a change in the configuration file: it is now similar to MWB Config file.
    For NL models, case normalization (transformations for changing case) is now available as a separate transformation step. This results in a change in the configuration file: it is now similar to MWB Config file.
    For NL models, allow having multiple transformations of the same type. This results in a change in the configuration file: it is now similar to MWB Config file.

FlashML Internals

    Lot of changes in the FlashML codebase behind the scenes, in order to optimize development and execution. Some of these are:
    We moved from SBT build system to maven build system
    The code now follows SparkML library style more closely by using pipelines
    
## FlashML Release 2018.2
### Features

FlashML Spark modules have been upgraded to the new released 2.3 version.
Data Loading

    FlashML now supports loading data from local filepath (in addition to HIVE and VERTICA) in CSV, TSV and JSON formats. The user needs to mention the same in the config file parameter "project.data.location" where user values have to be entered in the required key, in the JSON format.

Core Algorithms and Optimization

    Platt Scaling for multi-intent SVM: FlashML generates probability scores for multi-intent models built using SVM.
    Support to export top 3 intents for each row (utterance)

User-centric optimizations & Innovations

    The configuration file has been optimized and now has fewer parameters for the user to fill.
    Users can now control the level of Apache log messages during the jobs (some options include ERROR, INFO, OFF, ALL).
    Users need not have to fill client, version number, etc, rather, a single unique value for the project will suffice. (The value must remain unique to avoid over-writing of other jobs)
    To ensure uniformity and readability, the new folder structure is as follows:  /<Root Directory>/<project id>/<RetrainID>/*
    Certain parameters in the config file have been renamed for clarity. With regards to the naming convention of parameters in the configuration file, the terminology ‘project’ is used to reference all variables unique to the concerned project, while variables with the term ‘flashml’ are to be unique among different projects (to avoid overwriting to the pre-existing model jobs). Running jobs is now easier to view on console, with process related messages now moved to log (instead of being printed)
    Support for additional model type 'MONITORING' to prevent user initiated 'predict' workflows from being over-written by monitoring workflows.    
    
## FlashML Release 2018.1

### Features
Natural Language Processing (NLP)

    Skip-grams/AND-Rules
    TF-IDF

Core Algorithms and Optimization

    We now accept SiMOD joined view as the only source of data for model building, and require the users to specify queries that would transform/filter the input data. This helps us save the complete data chain for an individual model.
        Using any other arbitrary data source from BDP for model building is still supported.
    JS Support for publishing SVM models (with binary intent)
    Predict workflows now store data separately from training workflows
    Implemented Kryo Serialization for caching and shuffle operations - this speeds up training jobs significantly
    Computation of standard metrics are now faster
    Bug fixes in JS script for CountVectorizer   
    
## FlashML Release 2017.4

This is the first release of FlashML. With this release, we are adopting a quarterly cadence of release cycles, with the possibility of releasing bug-fixes as and when required. This release uses features from the recently released version of Apache Spark (v2.2), a distributed computing framework for big data.

### Features

Core Machine Learning Features

We have focused on making available some of the popular ML models:

Binary Classification Models:

    Logistic Regression
    Naive Bayes
    SVM

Multi-class Classification Models:

    Multinomial Logistic Regression
    SVM, using one-vs-rest

Following methods are available in FlashML for vectorizing features and projecting to higher dimensional space. These are wrappers on top of core SparkML features.

    Word2Vec (basic support)
    CountVectorizer
    HashingTF

We have also extended coverage for training the following type of models, frequently used with journey data:

    Page-level models
    Uplift models

Support for Natural Language Processing (NLP) Models

One of the important use case for moving to Spark was to be able to use the same codebase for training models on structured data and Natural Language models. FlashML has complete support for multiple preprocessing steps that are typical for NLP models:

    Tokenization
    Word-class and contraction (e.g., apostrophe) replacement
    Lemmatization and Stemming
    Stop-word removal
    n-gram creation

Publishing models in JS

Model generation includes JS code for using CountVectorizer and HashingTF in real-time. JS scripts can be generated for:

    All ML models
    Page-level models
    Uplift models

Other Features related to Model Training

    Support for stratified sampling for multi-class classifications
    Support for coverage cut-off for classes: if the count of labeled sample for a class is low, it  can be automatically clubbed with a 'default' class (e.g., 'other') for training purposes
    Support for k-fold cross-validation
    Support for time-on-page (TOP)-based simulation for models with journey data
    Support for dealing with class imbalance (through over- and under-sampling)
    Support for randomizing data based on a column
    Sensible defaults for all configurations used in model building

FlashML on Docker

We have a docker-ized version of PraaS available for two purposes: anyone can use this version and run model training workflow on their system without going through the process of installing multiple dependencies. We are also using this system ourselves for running automated unit-tests.

### Our Innovations

We had to innovate on multiple fronts in order for us to match features that were available in legacy Vertica-based PraaS or to advance the state-of-the-art. Here is a partial listing:

    Computations of probability scores for SVM models using Platt Scoring technique
    Research on dropping support for text-based custom variables in favor of using hashingTF
    Caching (we call 'SavePointing') the intermediate steps efficiently so that the workflow runs from multiple starting points

Known Issues and Limitations

    Computing custom metrics fail for very large dataset (~5GB or more) [Fixed in FlashML Release 2018.1]
    Optimization for thresholds for models with journey data is not available    
