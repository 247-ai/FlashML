# Configuration File documentation - FlashML

The parameters in the FlashML configuration file in a JSON format are listed here.
### Naming convention of parameters
Since the parameters listed follow a JSON structure, the nested parameters are represented in order with a dot. For example the nested JSON structure listed below is represented as a parameter name `flashml.root.directory`

As a quick use guide, please refer to the
[Beginner's Guide to FlashML](Beginner's%20guide%20to%20FlashML.MD)

The
[FlashML Comprehensive User Guide](The%20comprehensive%20FlashML%20user%20guide.MD) has been written to provide users a detailed account of all FlashML supported customizations and features.

<br />

For the block `experiment` (all the parameters listed below would have to be prepended by `experiment.`):


| Parameter | Accepted values   |   Remarks
|:------------------|:-------------------|:---------------------|
|parallelism|Accepts integer|Default value is 3, if not mentioned in the config file.|
| modelingMethod| Accepts an array of strings. Accepted values in the array include: (Case insensitive) <br /><ul><li>`single_intent`</li><li>`uplift`</li><li>`page_level`</li><li>`multi_intent`</li></ul>   | customized workflow path based on use case and prediction (subject to availability of mentioned features in input Data)|
| type    | Accepted values include: (Case insensitive) <br /><ul><li>`model`</li><li>`predict`</li><li>`monitoring`</li><ul>    | This support allows the user to use a model generated once, multiple times for testing purposes as different jobs, rather than overwrite model training jobs
|variables.scope|Accepted values include: (Case insensitive) <br /><ul><li>`allPage`</li><li>`NoPage`</li><li>`PerPage`</li></ul>| Users can choose from the supported option to build their models. This approach allows users to smoothly build multi page level models.|
|preprocessing.scope|Accepted values include: (Case insensitive)<br /><ul><li>`allPage`</li><li>`NoPage`</li><li>`PerPage`</li></ul>|Scope declaration option applicable only to Preprocessing section of the config file|
| featuregeneration.scope|  Accepted values include: (Case insensitive) <br /><ul><li>`allPage`</li><li>`NoPage`</li><li>`PerPage`</li></ul>| Scope option applied only to the Feature Generation section of the config file.|
| vectorization.text.scope         |  Accepted values include: (Case insensitive) <br /><ul><li>`allPage`</li><li>`NoPage`</li><li>`PerPage`</li></ul>                          |       Scope option applied only to the Vectorization section of the config file.
| retrainId                                      | Not a user-filled Value     | To be filled in by API
| pagelevel.numberOfPages    | Integer value. Number of Pages (if Page Level Model)    |  Default fixed at 4 pages
| numericVariables   |  Accepts array of array of Strings (page wise inlusion of said variables)      |  columns in table with numerical values
| categoricalVariables   |    Accepts array of array of Strings (page wise inlusion of said variables)       | columns in table with categories of fixed text values
| textVariables    |  Accepts array of array of strings (page wise mention of said variables)    | columns in table with mostly non-repeating text values
| primaryKey   | Accepts array of String (unique columns in the table)      | Mentioned columns should exist in the hive loaded  table
| responseVariable      | Accepts string      | output column/ required prediction column
| pageVariable    |   Accepts String   | column in table denoting page number
| dateVariable|  Date value is accepted (as any one of the many accepted standard date formats) | used in QA
| randomVariable| name for new variable generated randomly|
| randomNumberGeneratorVariable                  | Applied on an existing column, to generate random IDs   | Users must ensure the first 8 characters are hexadecimal
| uplift.treatmentVariable|   |Column name used for uplift
| cumulativeSessionTime  | String accepted (variable)  |   new column name, this is will store exact time spent on each page
| additionalVariables    | Array of strings accepted    | columns not used in model building, but needed for reporting or insights.,etc.
| customFilter   | SQL type variable conditional value          | To check for any filter conditions in column values (for eg null check)
| postPredictFilter  | String accepted (condition involving minimum one column)  | Used in Cross Validation wherein the input dataset is first transformed (and then CV is carried out for each fold)
| sample.type                                    |  Accepted values include: (Case insensitive) <br /><ul><li>`random` </li><li> `conditional` </li><li> `stratified` </li><ul>
| sample.split         |   Array of two integers accepted   | 80, 20 (for example)
| cv.folds      |     Integer value accepted. Indicates the number of required Cross Validation Folds      | If the value is 0, or 1, CV is Disabled. CV is enabled with the specified integer value (if the entered value is greater than 1)
| cv.predictSavePoint           |   Accepts Boolean value     | If set to `true`, the prediction datasets will be saved for each CV sub-fold model.
|cv.metric | Accepted values (case-insensitive): <br /> <ul><li>`accuracy`</li><li>`weightedPrecision`</li></li>`weightedRecall`</li><li>`f1`</li></ul> | The metric for choosing the best model among the CV experiments.
| hyperparamop           |   Boolean     | (Optional) If set to `true`, hyperband will be used for hyperparameter optimization, any configurations specified for cv, including cv.folds will be ignored
| hyperband.iterations           |   Integer      | (Optional) It is used to specify the no of configurations to search for when using hyperband, default_value:- 81|
| hyperband.iterMultiplier           |   Integer      | (Optional) It is used to specify no of spark iterations per hyperband iteration, default_value:- 20|  
| hyperband.eta           |   Integer      | (Optional) It is used to specify downsampling rate for hyperband, default_value:- 3|    
| hyperband.trainSize           |   Float      | (Optional) It is used to specify fraction of data used for training model using hyperband, remaining fraction is used as validation set. default_value:- 0.8|      
| sample.stratified.minimumClassSupport          | Integer value
| sample.stratified.otherClassValue              | Column name for which stratified sampling condition is applied
| sample.stratified.minimumClassSupportRequired  |  `true`or `false`
| sample.condition                               | value<fixed_value,value>fixed_value
| data.positivePercent                           | Integer type number, denoting the number
| featuregeneration.binning  |  Accepts pagewise listing of binning for numerical variables (2 dimensional array of JSON maps) | User must take care to ensure corresponding page has the required columns
| featuregeneration.grams   | Accepts pagewise listing of text processing options.  | Users must denote the transformations clearly, and ensure no naming conflict for the output variables at the page level
| preprocessing.steps                            | <pre>{<br />  "experiment": {<br />    "preprocessing": {<br />      "steps": [<br />        [<br />          {<br />            "inputVariable": "chatContent",<br />            "outputVariable": "chatContent1",<br />             "transformations": [<br />              {<br />                "type":"tokenizer",<br />                "parameter":"/"<br />              }<br />            ]<br />          }<br />        ]<br />      ]<br />    } <br />  } <br />} </pre>
| vectorization.categorical.method          | Accepted values include: (case insensitive) <br /> <ul><li>`hashingtf`</li> <li>`count_vectorizer`</li><li>`word2vec`</li> <li>`tfidf`</li></ul>
| vectorization.categorical.slots           | 1000 (for example)
| vectorization.text.steps                  | | Supports all text variable columns only, with one of following text processing methods (hashingtf, count_vectorizer, word2vec, tfidf) in a JSON Array format
| algorithm.type    | Accepted values include: (case insensitive) <br><ul><li>`logistic_regression`</li><li>`naive_bayes`</li><li>`svm`</li>
| multiIntent.topIntentsColumn    |    String accepted     |  new column to store intents
| multiIntent.maxTopIntents    |  Integer accepted        |  Maximum number of intents to be listed in order of their probability values
| experiment.algorithm.svm.plattScalingEnabled	|	Boolean accepted			|	Used in case of SVM based workflows. Optional for Platt Scaling. This option must be enabled if the 	`TopK Intents` feature is required.
| algorithm.build.type                           | Accepted values include: (Case insensitive) <br/> <ul><li>`binomial`</li><li> `ovr` (one-vs-rest)</li>
| algorithm.logistic.regparam                    | Logistic Regression Parameters                                                                                                                                                                                                                                                                                                                                                             | Spark Documentation here at https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.classification.LogisticRegression
| algorithm.logistic.maxiter                     | Logistic Regression Parameters
| algorithm.logistic.elasticNetParam             | Logistic Regression Parameters
| algorithm.logistic.standardization             | Logistic Regression Parameters
| algorithm.svm.regparam                         | SVM Parameter                                                                                                                                                                                                                                                                                                                                                                              | spark related config. Spark Documentation here at https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.classification.LinearSVC
| algorithm.svm.maxiter                          | SVM Parameter
| algorithm.svm.standardization                  | SVM Parameter
| customMetrics.type                             | CustomProbAndTopBased (or) CustomProbBased
| customMetrics.topvariable                      | The column name which contains time spent pagewise
| customMetrics.toplist                          | user given thresholds (for TOP)
| customMetrics.thresholds                       | for probability thresholds
| publish.format  | String value  | expected entries:  `JS`
| publish.online.pages     | Array of Integer values, specifying the page numbers for which JS script should be generated
| publish.thresholds  | Array of Double values accepted   |  The probability threshold values for each page are to be mentioned here
| publish.toplist   | array of integers accepted   |     The time on page threshold values are mentioned here
| publish.defaultIntent          | String value accepted. The default intent          |    This option is required only in the case of generating deployment ready script for Natural Language based prediction models, as a classification label for all out-of-domain intents.
|  publish.precision			|    Positive Integer value expected 	|	If the key is present and it is a positive value, user entered value will be accepted. Else, a default value of 8 will be taken.



For the rest of the parameters:

| Parameter |   Accepted Values | Remarks
|:--------|:-----------|:-----------------|
| hdfs.nameNode.uri                                         | hdfs://hadoop-master:9000                                                                                                                                                                                                                                                                                                                                                                  | hadoop-master needed for parallel computation of job among different machines.
| hive.thrift.url                                           | thrift://hadoop-master:9083                                                                                                                                                                                                                                                                                                                                                                | Local host needed for stand alone machine computation on local HDFS
| pipeline.steps        | Accepts an array of strings listing all steps the program should follow. Accepted values include: (Case insensitive) <ul><li>`dataReader`</li><li>`sampling`</li><li>`preprocessing`</li><li>`featureGeneration`</li><li>`Vectorization`</li><li>`modelling`</li><li>`scoring`</li><li>`StandardMetrics`</li><li>`CustomMetrics`</li><li>`Publish`</li><li>`QADataGeneration`</li></ul>  | The required steps to be followed are to be mentioned here, although in any order.
| savePointing.required                                     | `yes` or `no`      |       If chosen, intermediate datasets will be saved, user can run few steps of pipeline at a time.Even if user halts job at any instant, can resume pipeline from previous pipeline step (the steps have to however be mentioned in the Configuration file)
| flashml.root.directory     | HDFS project root Directory |  The base path for the current Spark Job
| project.data.location                                     | <pre>{<br />   "source": "hive://",<br />   "format":"", <br />   "temp_table_prefix": "praas_table_",<br />   "queries": ["SELECT vid,active_session,dt,session_time,purchase_flag FROM FLASHML.SEARSHS_TEST_DATA" ] <br /> }</pre>               | Supports loading of data files locally, HIVE tables, as well as query-type selection of data from HIVE table.
| project.id                                                | to be filled in by API                                                                                                                                                                                                                                                                                                                                                                   | unique identifier for a project
| project.log.level                                         |  Accepted values include: (case insensitive) <ul><li>`ERROR`</li><li>`ALL`</li><li>`OFF`</li><li>`INFO`</li><li>`DEBUG`</li><li>`FATAL`</li><li>`WARN`</li><li>`TRACE`</li></ul>  |This option will instruct the program to only capture the specified log level information of job status. Size of job files may increase, options such as DEBUG and ALL will cause the Spark Job to print onto console all relevant information.
| project.data.schema.file    | String (filepath)       |      Used in Publish step. SIMOD view related schema for each variable
| vertica.host.url   | User specific authentication     |  User specific authentication
| vertica.jdbc.driver  | User specific authentication    |  User specific authentication
| vertica.user.name    | User specific authentication    |  User specific authentication
| vertica.user.password  | User specific authentication   |  User specific authentication
