# The comprehensive FlashML User Guide

## Introduction
FlashML is the in-house ML prediction model building tool built and maintained by the FlashML team of [24]7.ai's Data Science Group. New features and updates are packaged and released as a new version of the tool quarterly (as a minor version). Major versions are released yearly. The tool was designed to support the requirements of customizing the entire model lifecycle to build the ideal prediction model. FlashML also supports retraining of prediction models as well as testing the performance metrics of previously built models.

## FlashML use cases
FlashML serves as a prediction model building tool for the following use cases:
<ol>
 <li><strong>Web journeys predictions: </strong> to identify proactive intervention targets</li>
 <li><strong>Natural language</strong> based classification models</li>
</ol>

#### Quick Links
-   [How to run FlashML](#how-to-run-flashml)
-   [Understanding the configuration file](#understanding-the-configuration-file)
-   [Loading data for modeling jobs](#loading-data-for-the-model-jobs)
	- [Loading data from Hive](#loading-data-from-hive)
	- [Loading data from HDFS](#loading-data-from-hdfs)
-   [Pipeline Steps](#the-pipeline-steps)
	- [Understanding scope in FlashML](#understanding-scope-in-flashml)
	- [Step details](#step-details)
		-   [Data Reader](#datareader)
		-   [Sampling](#sampling)
		-   [Preprocessing](#preprocessing)
		-   [Feature Generation](#feature-generation)
		-   [Vectorization](#vectorization)
		-   [Modeling](#modeling)
		-   [Scoring](#scoring)
		-   [Standard Metrics](#standard-metrics)
		-   [Custom Metrics](#custom-metrics)
		-   [Publish](#publish)
		-   [QA Data Generation](#qa-data-generation)
-   [Miscellaneous](#miscellaneous)
    -   [Running a Predict Job](#predict-job)
    -   [Analysis of prediction DataFrame](#predictionDF)

## How to run FlashML
The user requires access to a spark cluster with hadoop configured. Apart from loading the FlashML jar file onto hadoop, and a configuration file specifying all the model parameters, the user only needs to load data using one of the many options supported by FlashML.

## Understanding the Configuration File
 The user can submit the required jobs by clearly mentioning all the required parameters on the Configuration file. FlashML suppports configuration parameters in a JSON format.
 The user may refer to the
 [FlashML configuration file documentation](Configuration%20parameters%20table.MD) for understanding the parameters and the values accepted therein.

## Loading data for the modeling jobs
FlashML supports loading data from HDFS, as well as Hive tables. It also has the ability to load the input data in Spark and run a few query to prepare data for model training. The data source is specificed in the parameter <code>project.data.location</code> and a few sample snippets are listed here for reference.

### Loading data from Hive
To load data from a hive table, the database name and the table name have to be specified. This is the preferred way of loading training data to FlashML, specially when some of the columns are non-string type. In order to load a dataset in CSV or similar format, please follow the hive documentation. Some sample scripts are available in <code>/scripts</code> subfolder.
 
##### Config. snippet 1. Loading of an entire table from Hive
```
{
"project":{
  "data":{
    "location":{
      "source":"hive://FLASHML.TEST_DATA"
      }
    }
  }
}
```

Alternatively, the user can also enter SQL type queries if the requirement is to extract only a few columns, based on some conditions, with the required names, etc.

##### Config. snippet 2. Loading required columns of data from Hive
```
{
  "experiment" : {
    "project" : {
      "data": {
        "location": {
          "source": "hive://",
          "format": "",
          "temp_table_prefix": "flashml_table_",
          "queries": [
            "SELECT vid,active_session,dt,purchase_flag,top_current FROM FLASHML.TEST_DATA"
          ]
        }
      }
    }
  }
}
```

The parameter <code>project.data.location.queries</code> accepts an array of SQL query strings. When multiple SQL queries are entered, The queries are processed in order of the array indices. The parameter <code>project.data.location..temp_table_prefix</code> denotes the table name that will be used to denote the intermediate tables used when multiple queries are submitted. The output of a previous query can be referred to in the subsequent queries as <code>{temp_table_prefix}_1, {temp_table_prefix}_2</code> and so on as shown below. The output of the final query would be used as the input data to FlashML.

##### Config. snippet 3. Loading segmented data from a Hive

```
{
  "experiment":{
    "project" : {
      "data": {
        "location": {
          "source": "hive://",
          "format": "",
          "temp_table_prefix": "flashml_table_",
          "queries": [
            "SELECT vid,active_session,dt,no_of_visits_in_last_7_days,session_time FROM FLASHML.WebJourney_TEST_DATA",
            "SELECT vid,dt from flashml_table_1"
          ]
        }
      }
    }
  }
}
```

### Loading data from HDFS

FlashML also supports reading tabular structured data files, such as <code>CSV</code> and <code>JSON</code> files from an HDFS cluster. The user can specify the file name with the full path as shown below.
```
{
  "experiment": {
    "project": {
      "data": {
        "location": {
          "source": "hdfs:///user/cloudera/folderTrain/dataFile.csv"
        }
      }
    }
  }
}
 ```
Note: The columns of a CSV file loaded from HDFS can only be treated as string. If some of the columns have other datatypes, it is recommended to use a JSON file, or load the data through hive or JDBC. 

## The pipeline steps

Here the different steps regarding the model job will be elaborated in sequence wise steps. These steps are to be listed in the `pipeline.steps` parameter in the configuration file.

In pipeline steps, FlashML supports scope-processing of user-entered parameters allowing entry of variables and variable-transformations once in the case of multi-page level models. This method allows users the flexibility to declare variables and transformation methods in multi-page level models, considerably reducing the size of the JSON config file and enhancing the user experience. A few sample configuration snippets have been listed here to demonstrate the flexibility to declare variables.

### Understanding scope in FlashML
The key `experiment.variables.scope` accepts the following options (case insensitive):
<ul>
 <li><strong>allPage:</strong> This option allows the user to enter variables for a multi-page level model where the variables are the same for all pages</li>
 <li><strong>noPage:</strong> This option allows the user to enter variables for a non-page level model</li>
 <li><strong>perPage:</strong> This option allows the user to custom-specify required variables for a multi-page level model</li>
</ul>

##### Config. snippet 4. noPage scope option here pertains to a non-page level model
```
{
  "experiment": {
    "variables": {
      "scope": "noPage",
      "text": [
        "current_page_url",
        "chatText"
      ],
      "categorical": [
        "browser_cat",
        "deviceType"
      ],
      "numerical": [
        "salary",
        "bonus"
      ]
    }
  }
}
```

For the Config. snippet 2 below, the scope has been listed as `allPage`. The parameter `experiment.pageLevel.numberOfPages` denotes the number of pages, for which these variables will be used. Additionally, the user is required to enter the option `page_level` in the parameter `experiment.modelingMethod`.
##### Config. snippet 5. allPage scope option for variables declaration
```
{
  "experiment": {
    "variables": {
      "scope": "allPage",
      "text": [
        "current_page_url",
        "referrer"
      ],
      "numerical": [
        "no_of_visits_in_last_7_days"
      ],
      "categorical": [
        "initial_referrer_cat",
        "browser_cat",
        "isp",
        "os_cat",
        "dd",
        "hour_of_day"
      ]
    },
    "modelingMethod": [
      "single_intent",
      "page_level"
    ],
    "pageLevel": {
      "numberOfPages": 4
    }
  }
}
```

The `perPage` scope option in the Config. snippet 3 below allows the user to flexibly enter variables at a page level. The users are required to ensure that the option `page_level` is entered in the key `experiment.variables.modelingMethod`. Additionally, the key `experiment.pageLevel.numberOfPages` expects an integer value for the number of pages.
##### Config. snippet 6. Per Page scope option for variables declaration
```
{
  "experiment": {
    "variables": {
      "scope": "perPage",
      "text": [
        [
          "current_page_url",
          "referrer"
        ],
        [
          "chatText"
        ],
        [
          "chatText",
          "referrer"
        ]
      ],
      "numerical": [
        [
          "no_of_visitors"
        ],
        [
          "price"
        ],
        [
          "price",
          "no_of_visitors"
        ]
      ],
      "categorical": [
        [
          "browser",
          "deviceType",
          "hour_of_day"
        ],
        [
          "deviceType",
          "hour_of_day"
        ],
        [
          "deviceType"
        ]
      ]
    },
    "modelingMethod": [
      "single_intent",
      "page_level"
    ],
    "pageLevel": {
      "numberOfPages": 3
    }
  }
}
```
Scope declaration of config parameters is also supported in the `Preprocessing`,`Feature Generation` and `Vectorization` modules, to provide for a better user experience.

In general, support for the scope declaration for the future sections depends on the variables' scope option given by the user. The feature transformation steps include: `Preprocessing`, `Feature Generation` and `Vectorization`.

|   Feature transformation scope option    |  Variable Declaration option: allPage        |   Variable Declaration option: perPage | Variable Declaration option: noPage |
|:------------------|:-------------------|:---------------------|:------------------------|
|   allPage   |        Yes           |          Yes                 |        No               |
| perPage    |        Yes           |                 Yes          |                  No         |
|   noPage     |         No            |             No                 |         Yes                  |

### Step details

<ol>
<li><h4 id="datareader">Data Reader</h4> The parameter <code>project.data.location </code> must be filled. The Spark job reads the data source listed in this parameter and uses the loaded datasets for subsequent steps. For description, see the section "Loading data for modeling jobs".</li>

<li> <h4 id ="sampling"> Sampling</h4>

We offer a few methods for splitting the dataset into train and test sets. The parameter `experiment.sample.type` accepts the following values: (Case insensitive) <br />
<ul>
	<li><code>RANDOM</code></li>
	<li><code>STRATIFIED</code></li>
	<li><code>CONDITIONAL</code></li>
</ul>

The simple random sampling can be used to split

##### Config. snippet 7. Example of random sampling
```
{
  "experiment": {
    "sample": {
      "type": "random",
      "split": [
        70,
        30
      ]
    }
  }
}
```

We can also create a sample based on the value of a specific column, as shown in the snippet below. This is useful when we have a column which is already randomized.

##### Config. snippet 8. Example of conditional sampling
```
{
  "experiment": {
    "sample": {
      "type": "conditional",
      "condition": [
        "rv<=80",
        "rv>80"
      ]
    }
  }
}
```

Stratified sampling is used when we would like to have approximately same distribution of classes between the training and the test set. Naturally, this is typically used for multi-class classification problems.

We can specify a minimum count (also called support) for a specific class in `stratified.minimumClassSupport`. If the example row count in the training set for a particular class is lower than this value, the class string would be converted to the string provided by `stratified.otherClassValue`. This feature is off by default, and can be turned on by setting `stratified.minimumClassSupportRequired` to `true`.

##### Config. snippet 9. Example of stratified sampling
```
{
  "experiment": {
    "sample": {
      "type": "stratified",
      "split": [
        70,
        30
      ],
      "stratified": {
        "minimumClassSupport": 10,
        "otherClassValue": "other-other",
        "minimumClassSupportRequired": false
      }
    }
  }
}
```

Users can read more about sampling with these resources
<ul>
	<li> https://spark.apache.org/docs/current/mllib-statistics.html#stratified-sampling </li>
	<li> https://en.wikipedia.org/wiki/Sampling_(statistics) </li>
</ul>
</li>

<li><h4 id="preprocessing">Preprocessing</h4>

The parameter `experiment.preprocessing.steps` denotes the preprocessing of the dataset variables. The order of steps is preserved here, and changing the order of steps followed will cause change in intermediate dataset and ultimately model performance. In case of models for web journeys that involve multiple pages, this block allows users to specify pagewise preprocessing of data.

Scope declaration of config parameters is also supported in the `preprocessing` module. Users need to note the following conditional support options with regard to the scope option used while declaring variables.
If `noPage` option was used to declare variables, preprocessing scope option will also have to be `noPage`.
Similary, if `allPage` option was used to declare variables, preprocessing scope option could either be `allPage` or `perPage`.
Lastly, if `perPage` option was used to declare variables, preprocessing scope option could either be `allPage` or `perPage`.

A few sample config snippets are listed to demonstrate scope option declaration in preprocessing config. section of the config file. The individual preprocessing methods are themselves elaborated below, after the note on scope declaration.

In the case of a repetitive multi page level model, the preprocesing config scope can be set to `allPage`, allowing the user to enable the same config parameters are applied to all pages uniformly.

##### Config. snippet 10. allPage scope Preprocessing

```
{
  "experiment": {
    "preprocessing": {
      "scope": "allPage",
      "steps": [
        {
          "inputVariable": "current_page_url",
          "outputVariable": "current_page_url1",
          "transformations": [
            {
              "type": "nullcheck",
              "parameter": ""
            },
            {
              "type": "tokenizer",
              "parameter": "/"
            }
          ]
        }
      ]
    }
  }
}
```
The following option allows the user to declare the preprocessing config directly for a single page level model

##### Config. snippet 11. noPage Preprocessing Scope

```
{
  "experiment": {
    "preprocessing": {
      "scope": "noPage",
      "steps": [
        {
          "inputVariable": "current_page_url",
          "outputVariable": "current_page_url1",
          "transformations": [
            {
              "type": "nullcheck",
              "parameter": ""
            },
            {
              "type": "tokenizer",
              "parameter": "/"
            }
          ]
        }
      ]
    }
  }
}
```

The perPage option allows the user to custom-specify preprocessing config parameters for multi-page level models.

##### Config. snippet 12. perPage scope level for Preprocessing

```
{
  "experiment": {
    "preprocessing": {
      "scope": "perPage",
      "steps": [
        [
          {
            "inputVariable": "current_page_url",
            "outputVariable": "current_page_url1",
            "transformations": [
              {
                "type": "nullcheck",
                "parameter": ""
              },
              {
                "type": "tokenizer",
                "parameter": "/"
              }
            ]
          }
        ],
        [
          {
            "inputVariable": "chatColumn",
            "outputVariable": "chatColumn1",
            "transformations": [
              {
                "type": "nullcheck",
                "parameter": ""
              },
              {
                "type": "tokenizer",
                "parameter": "/"
              }
            ]
          }
        ]
      ]
    }
  }
}
```

For **text columns**, it is required that the last transformation step be tokenization so as to convert the data into an array of tokens, and then follow subsequent steps. The transformations supported are listed below, and the way they needs to be mentioned are provided in parenthesis.
<br />
</li>

<li><h4>Stopwords</h4> (<code>stopwords</code>)

FlashML allows the user to provide an array of words (strings) as the parameter. Alternatively, the user can also submit a text file with the stop words in each line of the text file. To submit a text file, the parameter should be the filepath (a string) and the spark-submit command should include the file. Examples for submitting the parameters as per both supported approaches are included in the sample config snippet number 6 below. The snippets below shows the options that can be used.
 ```
 {
   "type": "stopwords",
   "parameter": "support_files/stopword_english.txt"
 }
 ```
 ```
 {
   "type":"stopwords",
   "parameter":["i","hi","they","us"]
 }
 ```
</li>

<li><h4>Tokenizer</h4> (<code>tokenizer</code>)

The string that is to be applied as a delimiter has to be specified in the `parameter` key. Tokenization has to be mandatorily applied as the last preprocessing step (in case of any text preprocessing transformations). Sample config snippet num. 6 below lists an example of tokenization.
</li>

<li><h4>Stemming</h4> (<code>stemming</code>)

The `parameter` key allows the user to enter any specific exceptions to be followed while Stemming. Users can enter an array of strings, or a filepath (as a string) in the <code>parameter</code> key. If a file (listing exceptions) is to be used, each line of the text file should contain a word from the list. Alternatively, a blank string can be used to denote no specific exceptions to be enforced while stemming.

The stemming function is not supported by FlashML's Publish module at this time. Users can pick this preprocessing option and build prediction models and run upto Metrics steps.
</li>

<li><h4>Regular Expression Replacement</h4> (<code>regex_replacement</code>)

FlashML allows users to make text-replacements with labels by specifying a regular expression. For all text patterns matching the expression, the patterns will be replaced with the specified label. Users can either define the key-value pairs in a 'support file' or define it directly in the configuration file.

 In case of the file approach, users are required to enter the regex pattern and then the label (with tab spaced separation). A few lines are listed here for reference. In this example, we are replacing certain words with the word `"class_hello"`, followed by replacing all the occurances of email addresses with the word `"class_email"`.

 ```
 \\b(hola|hello|hi|hey)\\b   class_hello
 \\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}\\b   class_email
 ```
 Alternatively users can directly define the hashmap in the `parameter` key. In the following example snippet, we show the same use case explained above.

 ```
 {
  "type":"regex_replacement",
  "parameter": [
	  {
		"\\b(hola|hello|hi|hey)\\b":"class_hello"
	  },
	  {
		"\\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}\\b":"class_email"
	  }
  ]
}
 ```
 In the following example snippet, we show an example of `regex_replacement` operations which involve extracting out certain part of the input string and use them as the replacement.

The listed regex replacement works to replace any occurrences of the strings `MSIE` and `Chrome` with the string `Others`. Then, any occurrences of the string `Safari` are replaced with `Safari1`.

It must be noted that the following example demonstrates the application of regex patterns in the specified order. The regex patterns used here denote a case-sensitive approach.
```
{
   "inputVariable":"browser_cat",
   "outputVariable":"browser_cat1",
   "transformations":[
      {
         "type":"regex_replacement",
         "parameter":[
            {
               "MSIE|Chrome":"Others"
            },
            {
               "Safari":"Safari1"
            }
         ]
      },
      {
        "type":"tokenizer",
        "parameter":" ""
      }
   ]
}
```

To follow a case-flexible approach, a certain prefix needs to be added to the regex patterns. FlashML supports the `ignore-case` approach to regex replacement (and also regex removal which is listed later in the preprocessing section).
The following code snippet (which is a modified version of the above config snippet) shows how FlashML supports case-insensitive regex replacement.

By placing the string prefix `(?i:)` the user specifies that regex replacement should follow the `ignore-case` option on the entire pattern.

Taking the current example, can now replace the following values with the label `Others`:
    <ul>
        <li> ```Chrome``` </li>
        <li> ```CHROME``` </li>
        <li> ```chrome``` </li>
        <li> ```MSIE``` </li>
        <li> ```Msie``` </li>
        <li> ```msie``` </li>
    </ul>

Only a few examples of the many possible combinations were listed, to elaborate the ignore-case option.

```
{
   "inputVariable":"browser_cat",
   "outputVariable":"browser_cat1",
   "transformations":[
      {
         "type":"regex_replacement",
         "parameter":[
            {
               "(?i:)MSIE|Chrome":"Others"
            },
            {
               "(?i:)Safari":"Safari1"
            }
         ]
      },
      {
        "type":"tokenizer",
        "parameter":" ""
      }
   ]
}
```
</li>

<li><h4>Regular Expession Removal</h4> (<code>regex_removal</code>)

This preprocessing step allows users to certain text patterns by specifying a regex. The `parameter` key accepts an array of strings, or a string (denoting a filepath). If a file is to be used, users should ensure regexes in each line of the file. Few lines from a sample text file listed for reference.

```
\\b(hola|hello|hi|hey)\\b
https?:\\/\\/(www\\.)?[\\-a-zA-Z0-9@:%._\\+~#=\\/]+
#(?=[^a-zA-Z0-9])
```

A sample config snippet on directly entering the regex-removal parameters in the configuration file.
```
{
    "type":"regex_removal",
    "parameter": [
        "\\b(hola|hello|hi|hey)\\b",
        "https?:\\/\\/(www\\.)?[\\-a-zA-Z0-9@:%._\\+~#=\\/]+",
        "#(?=[^a-zA-Z0-9])"
    ]
}
```

The following two config snippets demonstrates how FlashML supports the `ignore-case` option while applying regex patterns.
First let us consider a generic config snippet, wherein certain strings are to be removed (in order) from each of the values in the specified input column.

```
{
   "inputVariable":"browser_cat",
   "outputVariable":"browser_cat1",
   "transformations":[
      {
         "type":"regex_removal",
         "parameter":[
            "ri",
            "ox"
         ]
      },
      {
         "type":"tokenizer",
         "parameter":" "
      }
   ]
}
```

The above config snippet has been modified to the below listed snippet. The `ignore-case` prefix string `(?i:)` has been included at the beginning of the string pattern thus enabling a global ignore-case regex pattern to be applied on the input string values.

```
{
   "inputVariable":"browser_cat",
   "outputVariable":"browser_cat1",
   "transformations":[
      {
         "type":"regex_removal",
         "parameter":[
            "(?i:)ri",
            "(?i:)ox"
         ]
      },
      {
         "type":"tokenizer",
         "parameter":" "
      }
   ]
}
```

Users may kindly note that the specified regex operations will be included in the Javascript deployment file that is generated in the `Publish` option. The regexes will need to comply with Java regex and Javascript regex syntaxes, to ensure that the same pre-processing applied on the training input (during the model building phase) is applied on the model evaluation input.
</li>

<li><h4>Case Normalization</h4> (<code>case_normalization</code>)

The user is not required to pass any arguments in this preprocessing type. Refer the sample config snippet number 6 below.
</li>

<li><h4>Word Classes Replacement</h4> (<code>word_classes_replacement</code>)

This step allows users to define a label as a replacement for multiple string values. The `parameter` key allows the user to enter either a string (denoting a filepath) or a HashMap mapping the labels to arrays of strings (denoting the similar words that should be replaced with the common label). In case of the file approach, the HashMap can be directly entered in the text file. A few sample lines include:

```
{
    "_class_family" : ["son","son's","daughter","daughter's","wife","wife's","wives","spouse","spouse's"],
    "_class_number" : ["zero","one","two","three","four","five","six","seven","eight","nine"]
}
```

A sample config snippet listing the same input directly in the configuration file:

```
{
    "type": "word_classes_replacement",
    "parameter": {
        "_class_family" : ["son","son's","daughter","daughter's","wife","wife's","wives","spouse","spouse's"],
        "_class_number" : ["zero","one","two","three","four","five","six","seven","eight","nine"]
    }
}
```

The word-class replacement function is not supported by FlashML's Publish module at this time. Users can pick this preprocessing option and build prediction models and run upto Metrics steps.
</li>

<li><h4>Sentence Marker</h4> (<code>sentence_marker</code>)

No parameter is required from the user in this preprocessing step. This step adds the string `_class_ss_` at the beginning of the string, and the string `_class_se` at the end and returns the new string.
</li>

<li><h4>Contractions Replacement</h4> (<code>contractions_replacement</code>)

This step allows users to convert different forms of a word to a (user-defined) base form. Users may submit the replacements either as a string (denoting a filepath) with each line mapping the different forms of a word to it's required form, or, directly as a dictionary in the config file. Sample config snippet num. 6 listed below shows that a filepath has been depicted. Some sample lines from the file are listed here for reference:

```
stealthier,stealthy
stealthiest,stealthy
healthier,healthy
healthiest,healthy
luckier,lucky
luckiest,lucky
```

The contractions-replacement function is not supported by FlashML's Publish module at this time. Users can pick this preprocessing option and build prediction models and run upto Metrics steps.
</li>

<li><h4>Null check</h4> (<code>nullcheck</code>)

FlashML allows the user to replace `null` values in the column with a string value mentioned in the `parameter` field. For non-string data types also, the user may enter a string as the replacement value. FlashML internally completes the replacement and converts it to the data type as per the schema file. In the following example, we check for nulls in two different columns and treat them in different ways.
```
  {
	"inputVariable": "var1",
	"outputVariable":"var1_sanitized",
	"transformations": [
	  {
		"type":"nullcheck",
		"parameter":"tempText"
	  }
	]
  },
  {
	"inputVariable": "var2",
	"outputVariable":"var2_sanitized",
	"transformations": [
	  {
		"type":"nullcheck",
		"parameter":"0"
	  }
	]
  }

```
</li>
</ol>

FlashML follows a simplified way of listing the preprocessing steps (sample configuration snippet below).

Users are required to ensure that column names are unique at a page level. Across multiple pages, the same output column names can be repeated.
During each transformation step, the users are required to enter the `type` of preprocessing method. Excepting the preprocessing methods `case_normalization` and `sentence_marker` all the other preprocessing methods require the additional key `parameter`.

##### Config. snippet 13. Example of preprocessing configuration
```
{
  "preprocessing": {
    "scope": "noPage",
    "steps": [
      {
        "inputVariable": "lineText",
        "outputVariable": "lineText1",
        "transformations": [
          {
            "type": "nullcheck",
            "parameter": ""
          },
          {
            "type": "case_normalization"
          },
          {
            "type": "stopwords",
            "parameter": "support_files/stopword_english.txt"
          },
          {
            "type": "stopwords",
            "parameter": [
              "i",
              "hi",
              "they",
              "us"
            ]
          },
          {
            "type": "stemming",
            "parameter": []
          },
          {
            "type": "contractions_replacement",
            "parameter": "support_files/lemma.txt"
          },
          {
            "type": "tokenizer",
            "parameter": "[.,? ###_END_### !]"
          }
        ]
      }
    ]
  }
}
```

In case there is no pre-processing required, users need leave the steps blank with empty square parantheses, as shown below.

```
{
  "experiment": {
    "preprocessing" : {
      "steps": [ ]
    }
  }
}
```

In the case that users need to preprocess data only for certain number of pages, such a requirement can also be accommodated by FlashML. Consider the case where the user needs to preprocess a variable only on page 2 (for example). The configuration snippet listed below is an example for the same.

##### Config. snippet 14. Specific Page level preprocessing
 ```
 {
  "experiment": {
    "preprocessing": {
      "scope":"perPage",
      "steps": [
        [],
        [
          {
            "inputVariable": "chatContent",
            "outputVariable": "chatContent1",
            "transformations": [
              {
                "type":"tokenizer",
                "parameter":"/"
              }
            ]
          }
        ]
      ]
    }
  }
}
 ```

 Users can read more about preprocessing here at
<ul>
 <li>https://spark.apache.org/docs/2.4.0/ml-features.html </li>
 <li> http://www.cs.ccsu.edu/~markov/ccsu_courses/datamining-3.html </li>
 <li> https://nlp.stanford.edu/software/tokenizer.shtml</li>
 <li> https://en.wikipedia.org/wiki/Stop_words</li>
 <li> https://towardsdatascience.com/word2vec-skip-gram-model-part-1-intuition-78614e4d6e0b</li>
</ul>
<br />

<li><h4 id="feature-generation">Feature Generation</h4></li>
The parameter <code>featuregeneration.grams</code> should list the pagewise list of text transformations to be applied on the specified variables in order.
Support is provided for the keys <code>Ngram</code>, <code>SKIP_GRAM</code> (Case insensitive).

Scope declaration of config. parameters is also supported for feature generation config section.
The key `experiment.preprocessing.scope` allows the user to declare the config. parameters as per page level requirements.
Only certain combinations of feature generation scope are supported, based on the variables' scope option used.

If `noPage` option is used to declare variables, Feature Generation scope option can only be `noPage`.
Similarly if `allPage` option is used for variable declaration, Feature Generation scope option could either be `perPage` or `allPage`.
Lastly, if `perPage` option is used for variable declaration, Feature Generation scope option could be either `perPage` or `allPage`.

A few config. snippets covering the Feature Generation section of the config. file will be listed. The feature-generation options themselves are listed after below, after the note on scope declaration for this section.

The following config. snippet shows how the user can custom-specify binning options for a multi-page level model.

##### Config. snippet 15. Per Page Feature Generation
```
{
  "experiment": {
    "featuregeneration": {
      "scope": "perPage",
      "binning": [
        [],
        [
          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "intervals",
            "parameter": [
              0,
              25,
              50
            ]
          }
        ],
        [
          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "equiDistant",
            "parameter": 10
          }
        ],
        [
          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "equiArea",
            "parameter": 10
          }
        ]
      ],
      "grams": [
      ]
    }
  }
}
```

The `allPage` option allows the user to declare config. parameters for one page, efficiently implement the same for a multi-page level model.
The same config. snippet could also be used in the case of a non-page level model, provided that the scope option set is `noPage`.

##### Config. snippet 16. All Page Feature Generation
```
{
  "experiment": {
    "featuregeneration": {
      "scope": "allPage",
      "binning":
      [
          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "equiArea",
            "parameter": 10
          }
        ]
      ,
      "grams": [
      ]
    }
  }
}
```

In the case of `Ngram` the tokenized sequence of strings is divided into groups of `n` sized consecutive tokens and are stored as 2 dimensional arrays of strings taken in order, along with the parent one dimensional sequence of strings.
Windows are constructed from a default length of 3, upto the user specified parameter.
Let us see an example for the same. Consider the sequence of strings labelled 'LineText1' : `["The","assignments","are","due","next","Thursday"]`
<br />
Now let us apply the feature generation config. mentioned below in Config. snippet 8. <br />
Applying Ngrams of size 2 yields the Array of strings: `[("The","assignments"),("assignments","are"),("are","due"),("due","next"),("next","Thursday")]`
<br />
Applying Ngrams of size 3 yields the Array of strings: `[("The","assignments","are"),("assignments","are","due"),("are","due","next"),("due","next","Thursday")]`

The N-Grams function is not supported by FlashML's Publish module at this time. Users can pick this preprocessing option and build prediction models and run upto Metrics steps.

<br />
The above two intermediate generated columns are appended with the parent one dimensional sequence of strings and saved in the user specified parameter <code>outputVariable</code> for the page specific transformation in <code>experiment.featureGeneration.steps</code>.


##### Config. snippet 17. Sample Feature Generation block
```
{
  "experiment": {
    "featureGeneration": {
      "scope":"noPage",
      "steps": [
          {
            "inputVariable": "lineText1",
            "outputVariable": "lineText2",
            "transformations": [
              {
                "ngram": [
                  2,
                  3
                ]
              }
            ]
         }
      ]
    }
  }
}
```

In the case of `Skip Gram` the tokenized sequence of strings is used to build tuples with a distance of `n` tokens between each other, and these tuples are appended to the `inputVariable` and stored in the additional column name `outputVariable`. Refer config. snippet 9.
Consider the above declared sequence of strings labelled `LineText1`.
Windows are made from size 3, to the value specified in the parameter `skip_gram` and then the first and last elements are joined to form a string.
All such pairs are stored in a one dimensional array and renamed with the `outputVariable` parameter specified.
Applying Skip grams yields the following Array : `["The","assignments","are","due","next","Thursday","The & are","assignments & due","are & next","due & Thursday","The & due","assignments & next","are & Thursday"]`
<br />

##### Config. snippet 18. Sample feature generation block for Skip Grams
```
{
  "experiment": {
    "featuregeneration": {
      "scope":"noPage",
      "steps":
        [
          {
            "inputVariable": "lineText1",
            "outputVariable": "lineText3",
            "transformations": [
              {
                "SKIP_GRAM": 4
              }
            ]
          }
        ]
    }
  }
}
```

In case no text processing is required, the `featureGeneration.grams` can be left blank (i.e. represented by empty square parantheses). For example:
```
"featuregeneration": { "grams": [] }
```

FlashML also supports <code>Binning</code> of numerical variables at a pagewise level.
The types of binning supported are:
<ul>
 <li>Custom Intervals</li>
    The binning intervals can be directly entered in this option(in ascending order).
 <li>Equi - Distant Intervals</li>
    Users can enter the required number of buckets and the bins generated will have boundary intervals equidistant.
 <li>Equi - area Intervals</li>
    Users can enter the required number of buckets and the bins generated will have roughly the same number of values.
</ul>

The parameter <code>"featuregeneration.binning"</code> accepts a pagewise list of binning options to be applied to variables that the user declared in the numerical variables parameter.

Sample Config. snippets listed here will aid the user in selecting among the available options:

In the following config snippet, the binning boundaries are directly entered by the user, in ascending order.
FlashML adds -Infinity and +Infinity to the intervals in order and bins the values.

##### Config. snippet 19. Sample binning config. for intervals
```
"experiment": {
    "featuregeneration": {
      "scope":"noPage",
      "binning": [

          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "intervals",
            "parameter": [
              0,
              25,
              50
            ]
          }

      ]
   }
}
```

In the following config snippet, the user enters the required number of bins and the option <code>equiArea</code>
The bins are generated keeping the size of each bin roughly the same.
The boundaries are later saved and used in the <code>Publish</code> step for deployment.

##### Config. snippet 20. Sample binning config. for equi-area binning
```
"experiment": {
 "featuregeneration": {
      "scope":"noPage",
      "binning": [
       [
          {
            "inputVariable": "no_of_visits_in_last_7_days",
            "type": "equiArea",
            "parameter": 10
          }
        ]
     ]
   }
}
```

In the following config snippet, the user enters the required number of bins and enters the option <code>equiDistant</code>
The boundaries generated for the given input variable are equidistant.
The boundary intervals (also called splits) are saved for use in the <code>Publish</code> step for model deployment.

##### Config. snippet 21. Sample binning config. for equi-distant binning
```
"experiment": {
 "featuregeneration": {
   "scope":"noPage",
   "binning":
     [
       {
         "inputVariable": "no_of_visits_in_last_7_days",
         "type": "equiDistant",
         "parameter": 10
       }
     ]
  }
}
```

Users can read more about feature transformation with Spark here at <br /> https://spark.apache.org/docs/latest/ml-features.html#feature-transformers <br />
<li>
<h4 id="vectorization">Vectorization</h4></li>
The parameter block <code>experiment.vectorization</code> is to be filled listing a pagewise sequence of required extraction and transformation of features.

Scope declaration for text variables in the vectorization section of the config. file is supported. They key `experiment.vectorization.text.scope` accepts the conventional scope declaration options (case insensitive).

Using `allPage` scope option here allows the user to declare config. parameters for one page, and mark them to be repeated for all the other pages (Used for a multi page level model).
The same config. snippet listed below could also be used for `noPage` scope option when building a non-page level model, provided the option `noPage` is marked in the key `experiment.vectorization.text.scope`.

##### Config. snippet 22. allPage scope declaration
```
{
  "experiment": {
    "vectorization": {
      "text": {
        "scope": "allPage",
        "steps": [
          {
            "inputVariable": "current_page_url1",
            "method": "hashingtf",
            "slots": 2000

          },
          {
            "inputVariable": "referrer1",
            "method": "hashingtf",
            "slots": 2000
          }
        ]
      },
      "categorical": {
        "method": "hashingtf",
        "slots": 1000
      }
    }
  }
}
```

The following code snippet shows the user using the `noPage` option of scope declaration to custom specify vectorization steps at a multi-page level. In the example, there is no vectorization (of text columns) applied for page 2. (In the example, it is presumed that the user is building a 2 page level model)

##### Config. snippet 23. perPage scope for vectorization of text variables
```
{
  "experiment": {
    "vectorization": {
      "text": {
        "scope": "perPage",
        "steps": [
          [
            {
              "inputVariable": "current_page_url1",
              "method": "hashingtf",
              "slots": 2000
            },
            {
              "inputVariable": "referrer1",
              "method": "hashingtf",
              "slots": 2000
            }
          ],
          []
        ]
      },
      "categorical": {
        "method": "hashingtf",
        "slots": 1000
      }
    }
  }
}
```



##### Config. snippet 24. Sample config. for feature vectorization

```
{
  "experiment": {
    "vectorization": {
      "text": {
        "scope":"noPage",
        "steps":
        [
          {
            "inputVariable": "lineText2",
            "method": "hashingtf",
            "slots": 2000
          }
        ]
      },
      "categorical": {
        "method": "hashingtf",
        "slots": 1000
      }
    }
  }
}
```

The vectorization step converts the text content to numbers (each unique key assigned one number) and builds arrays of given size. For the above given example the vector built is of length 5000 (from the two text based variables from the categorical variable).

FlashML currently supports the following methods to generate term frequency vectors(Case insensitive):
<br />
<ul>
 <li><code>HASHINGTF</code></li>
 <li><code>COUNT_VECTORIZER</code></li>
 <li><code>WORD2VEC</code></li>
 <li><code>TFIDF</code></li>
</ul>

It is required that the number of slots selected (target feature dimension, i.e. number of buckets on the hash table) for the term frequency vectors are sufficient enough to avoid hash collision. Users can read more about the way Spark supports feature extraction and transformation with these resources:
<ul>
 <li>https://spark.apache.org/docs/latest/ml-features.html#countvectorizer</li>
 <li>https://spark.apache.org/docs/latest/ml-features.html#word2vec</li>
 <li>https://spark.apache.org/docs/latest/ml-features.html#tf-idf</li>
</ul>
<br />

<li><h4 id="modeling">Modeling</h4>

The parameter <code>experiment.modelingMethod</code> accepts an array of string values.
The accepted values are case insensitive and include:
<ul>
 <li><code>single_intent</code></li>
 <li><code>multi_intent</code></li>
 <li><code>uplift</code></li>
 <li><code>page_level</code></li>
</ul>
<br />

By <code>Single Intent</code> it is understood that the prediction model involves data of only two intents, usually used to identify proactive intervention targets.

The option <code>Multi Intent</code> is used in cases where the prediction model involves lines of multiple intents, usually used in NL cases to identify user intents.

The <code>Uplift</code> option is used in cases where it is likely that the participation of the a particular column (a separate parameter to be filled) boosts the prediction model's performance. Model performance metrics are generated with and without the additional variable, and the better of the two prediction models is considered as the output model for the job.
The <code>Uplift</code> option is to be entered in the parameter <code>experiment.modelingMethod</code>

The parameter `experiment.uplift.treatmentVariable` in the sample configuration snippet below shows that a column name of the specified dataset should be listed, and the `uplift` option should be included in the parameter `experiment.modelingMethod`

```
{
  "experiment" : {
    "uplift": {
      "treatmentVariable": "repeatVisitor"
    }
  }
}
```

The <code>Page Level</code> option is used in cases where the data extends to multiple pages and model performance metrics are also computed for each page, and co-efficients for the prediction model for each page are listed in the ready-to-deploy JavaScript file.
The majority of `Page level` models are used for Web journey data based models, to identify proactive intervention targets.

Following are the accepted array combinations for the parameter `experiment.modelingMethod`
<br />
<ul>
 <li>[<code>"single_intent"</code>]</li>
 <li>[<code>"single_intent"</code>,<code>"uplift"</code>]</li>
 <li>[<code>"single_intent"</code>,<code>"page_level"</code>]</li>
 <li>[<code>"single_intent"</code>,<code>"uplift"</code>,<code>"page_level"</code>]</li>
 <li>[<code>"multi_intent"</code>]</li>
 <li>[<code>"multi_intent"</code>,<code>"page_level"</code>]</li>
</ul>
<br />

The `uplift` option in the parameter `experiment.modelingMethod` will ensure that a model is built with the specified variable, and checked to see if the additional variable's participation improves the model performance.

The parameter block <code>experiment.algorithm</code> requires the type of algorithm to be filled in.
The parameter <code>experiment.algorithm.type</code> accepts the below listed values (Case insensitive).
<ul>
 <li>svm</li>
 <li>logistic_regression</li>
</ul>
<br />

The following configuration block pertains with the modelling step

```
{
  "experiment": {
   "cv": {
        "folds": 0,
        "predictSavepoint": false
      },
    "modelingMethod": [
      "single_intent",
      "page_level"
    ],
    "type": "model",
    "algorithm": {
      "type": "svm",
      "build": {
        "type": "binomial"
      },
      "svm": {
        "regparam": 0.01,
        "maxiter": 10,
        "standardization": true
      }
    },
    "pageLevel": {
      "numberOfPages": 4
    },
    "page_variable":"pgNum"
  }
}
```

FlashML supports Cross Validation on both the above listed algorithms.

In the below listed sample code snippet, cross validation is disabled as the number of folds is less than 1. The parameter `experiment.cv.folds` accepts the value of `k` for a  k-fold cross calidation (the integer `k` has to be greater than 1 to enable CV computation). Users can save the prediction datasets for each of the `k` subfolds by enabling the parameter `experiment.cv.predictSavepoint`. The saved datasets for the different subfolds helps the user identify the optimum intent acceptance thresholds.
```
{
  "experiment": {
    "cv": {
      "folds": 1,
      "predictSavepoint": true
    }
  }
}
```

In case of CV models, the configuration for SVM parameters should be modified, as listed in the code snippet below. Note that, here the parameters has to be given as an array of elements, even though there might be a single value for a parameter. FlashML will form a ParamGrid (grid of parameters and perform CV with each combination of the values. (In this example case, there will be 6 possible combinations applied to compute each CV fold). For non-CV models, none of the parameters need to be mentioned in arrays.

```
"svm": {
        "regparam": [0.01, 0.05, 0.09],
        "maxiter": [10, 50],
        "standardization": [true]
      }
```
FlashML supports hyperparameter tuning using [hyperband](https://arxiv.org/abs/1603.06560). It is available only for svm & logistic_regression. If hyperband is used then values specified under cv, including cv.folds won't be used. To use hyperband, `experiment.hyperparamop` should be set as true. 
```
  "experiment": {
      "hyperparamop": true
    }
```
Other **optional parameters** that can be modified for hyperband experiment are - 
```
"hyperband": {
      "iterations":81,
      "iterMultiplier":20,
      "eta":3,
      "trainSize":0.8
    }
```
For description about these parameters refer to - [Configuration parameters list](Configuration%20parameters%20table.MD)

If hyperband is being used, then `regparam` & `elasticnetparam` can be specified as either as a **single value**, **list** or as a **dictionary** with the following key values *min* & *max* - 
```
"elasticnetparam": 0.5
	
"elasticnetparam": [0.1,0.5,1]
	
"elasticnetparam": {
          "min":0.0,
          "max":1.0
        }

"regparam" : 0.1
	
"regparam": [0.01,0.1,1,10,100]
	
"regparam": {
          "min":0.0,
          "max":100.0
        }

```
*standardization* can be specified as a single value or as a list
*maxiter* **will have to specified as an int value**

```
"logistic": {
        "regparam":[0.01, 0.1, 1, 10, 100],
        "maxiter":1000,
        "standardization": true,
        "elasticnetparam": {
          "min":0.0,
          "max":1.0
        }
      }	
      
"svm": {
	"regparam": [0.01, 0.1, 1, 10, 100],
	"maxiter": 1000,
	"standardization": [
		true, false
	]
}      
```
The parameter `multiIntent.maxTopIntents` accepts an Integer value and the specified number of intents will be listed based on descending order of their probability values. These intents will be stored under the column name provided by the user for the parameter `experiment.multiIntent.topIntentsColumn`. Sample code block listed below for reference.

```
{
  "experiment" :{
    "multiIntent": {
      "topIntentsColumn": "top_intents",
      "maxTopIntents": 10
    }
  }
}
```

In this case, the new column name `top_intents` is created. This column contains a list of 10 intents listed in decreasing order of their probability values.

Users can read more about Spark support for ML algorithms with the resources listed below:
 <ul>
 <li>https://spark.apache.org/docs/latest/ml-classification-regression.html</li>
 <li>https://spark.apache.org/docs/2.4.0/ml-tuning.html</li>
 <li>https://en.wikipedia.org/wiki/Support_vector_machine</li>
</ul>
</li>

<li><h4 id="scoring">Scoring</h4>
It is expected that either of the two parameters <code>experiment.postPredictFilter</code> or <code>experiment.responseVariable</code> is populated.
The model built with the specified parameters is run on the input datasets.
Depending on the <code>savePointing.required</code> boolean parameter entered by the user, the prediction datapoints are saved on the HDFS project directory.
</li>

<li><h4 id="standard metrics">Standard Metrics</h4>
Once the standard metrics are computed, the scores are saved in the project directory on HDFS, at the location
```$"rootDirectory"+"/metrics/standardMetrics/stdMetrics.json"```

The following scores are computed as Standard Metrics for both the Train and Test Datasets:
<br />
<ul>
 <li>AUROC</li>
 <li>F2 scores</li>
</ul>

Users can read more about AUROC and related metrics at these resources.
<ul>
 <li>https://spark.apache.org/docs/2.3.1/mllib-evaluation-metrics.html</li>
 <li>https://in.mathworks.com/matlabcentral/fileexchange/19468-auroc-area-under-receiver-operating-characteristic</li>
 <li>https://stats.stackexchange.com/questions/132777/what-does-auc-stand-for-and-what-is-it</li>
</ul>
</li>

<li><h4 id="custom metrics">Custom Metrics</h4>

 The parameter <code>experiment.customMetrics.type</code> accepts the following String entries:
<ul>
 <li>customProbAndTopBased</li>
 <li>customProbBased</li>
</ul>
<br />
 The parameter <code>experiment.customMetrics.topVariable</code> accepts the string variable containing the required parameter. In this case FlashML contains the 'Time on page' value (i.e. time spent on each page).

 The parameter <code>experiment.customMetrics.topList</code> contains the pagewise lower bound threshold for the dataPoint to qualify as a 'proactive intervention target'.
 Also the pagewise lower bound threshold for probability is also used to screen dataPoints for qualifying as a proactive intervention target.
 The parameter <code>experiment.customMetrics.thresholds</code> accepts an array of Float values, denoting the pagewise probability thresholds.

```
{
  "experiment": {
    "customMetrics": {
      "type": "customProbAndTopBased",
      "topVariable": "top_current",
      "topList": [
        20,
        20,
        20,
        20
      ],
      "thresholds": [
        0.7,
        0.7,
        0.5,
        0.4
      ],
      "npages": 4
    }
  }
}
```
</li>

<li><h4 id="publish">Publish</h4>
In this step, a JavaScript file is generated which contains the model parameters (preprocessing, vectorization, model algorithms, information on pagewise parameters, if any) containing functions needed to operate on input data to generate required information.
The schema file path is to be loaded in the parameter <code>project.data.schema.file</code> and should contain the mapping of each of the variables to the specified format view .
</li>
<br />
<br />

Users can generate execution-ready scripts using the parameter `experiment.publish`
A sample snippet is listed here for reference.
The parameter `publish.format` accepts the values:
<ul>
<li> <code>JS</code> </li>
The option to generate execution ready scripts for web journey based prediction models.
</ul>
<br />

In case the user requires script to serve a page level model, the required values are to be specified for the respective page numbers. In this case, the user requires script to handle page numbers 1 to 4, operating on the probability thresholds specified (to be considered a priority-intervention-target).
The key `experiment.publish.precision` allows the user to custom set the co-efficients' precision of the trained classifiers which will be filled in the ready-to-execute deployment script.

```
{
  "experiment": {
    "publish": {
      "format": "js",
      "topList": [
        0.1,
        0.1,
        0.1,
        0.1
      ],
      "thresholds": [
        0.1,
        0.1,
        0.1,
        0.1
      ],
      "precision":10,
      "online": {
        "pages": [
          1,
          2,
          3,
          4
        ]
      }
    }
  }
}
```

Users can also specify the deployment script for a single page.
```
{
  "experiment": {
    "publish":{
      "thresholds":[0.2],
      "precision":4,
      "format":"JS",
      "online":{
        "pages":[1]
      }
    }
  }
}
```

Users may note that some Preprocessing and Feature Generation options are not supported in the Publish Step.
Unsupported Preprocessing transformation(s): Stemming, Contractions Replacement, Word class replacement
Unsupported Feature Generation transformation(s): NGrams

<li><h4 id="qa data generation">QA Data Generation</h4>

The Quality Analysis related data is saved at this step, in the specified format.
The parameter `experiment.qa.format` accepts the following string values (Case insensitive):
<ul>
 <li>json</li>
 <li>csv</li>
</ul>

The parameter `experiment.qa.dataPoints` accepts an Integer value and denotes the maximum number of visitors that can be saved (filtered pagewise, if applicable) and the output columns (primary keys, page variable, probability scores) for the selected data points alone, are saved as per the specified file format.

```
{
  "experiment" : {
    "qa": {
      "format": "csv",
      "dataPoints": 1000
    }
  }
}
```
</li>

## Miscellaneous
Certain additional topics of interest to users have been coverered here. Users can read the following to identify the Hadoop locations on which some data is written onto, after certain steps are computed on FlashML.

### Running a "predict" job on new dataset
Let's say, you have build a prediction model by running FlashMLand now you would like to test the model on some newer data (perhaps out-of-sample data). In this case the you need to enter the value `predict` (case insensitive) for the key `experiment.type` in the config file. FlashML will use the model built (by loading the pipeline, i.e. the result of the model building done on the previous training data) and will 'score' the data, generating predictions and metrics.

### Analysis of prediction dataframe
At the <code>scoring</code> pipeline step, FlashML generates predictions for the test data and then saves the entire dataframe in <code>parquet</code> format in the Hadoop directories assigned for the current project. The location of the directory for the current project is generated using parameters in the config file as follows: users can explore the hadoop path <code>/${FlashML.rootDirectory}/${project.id}/${model.id}/${experiment.retrainID}/${experiment.type}/</code> for accessing the current job related files. To load the result of the scoring dataset, the parquet files can be loaded onto a Spark DataFrame and analysed. Users should ensure that the Spark version used is the same as that of the currently used FlashML version. The command <code>spark.read.parquet(FilePath)</code> yields the DataFrame with all the required rows.

A sample config snippet is listed here for reference.

```
{
   "flashml": {
    "context": "yarn",
    "rootDirectory": "/flashml"
  },
  "model":{"id":"mdID"},
  "project": {
    "id": "pdID",
    "logLevel": "ERROR",
    "data": {
      "location": {
        "source": "hive://flashml.trainingDataSet",
        "format": "",
        "temp_table_prefix": "flashml_table_",
        "queries": [ ]
      },
      "schema": {
        "file": "schema.txt"
      }
    }
  },
  "job":{
    "id":"job1"
  },
  "experiment": {
    "retrainId": "original",
  "variables":{
	"scope":"noPage",
	"numerical": ["numberofVisitors"],
	"categorical": [ "device"],
	"text": []
	},
    "cv": {
      "folds": 0,
      "predictSavepoint":false
    },
    "modelingMethod": [
      "multi_intent"
    ],
    "type": "model"
   }
}
```

```
-bash-4.4$ hadoop fs -ls /flashml/pdID/mdID/original/model/noPage/data
Found 4 items
drwxr-xr-x   - myUser hdfs          0 2019-05-20 14:50 /flashml/pdID/mdID/original/model/noPage/data/scoringTest
drwxr-xr-x   - myUser hdfs          0 2019-05-20 14:50 /flashml/pdID/mdID/original/model/noPage/data/scoringTrain
drwxr-xr-x   - myUser hdfs          0 2019-05-20 14:35 /flashml/pdID/mdID/original/model/noPage/data/vectorizationTest
drwxr-xr-x   - myUser hdfs          0 2019-05-20 14:35 /flashml/pdID/mdID/original/model/noPage/data/vectorizationTrain
```
In this case the hadoop path `/flashml/pdID/mdID/original/model/noPage/data/ScoringTrain` is the required path to analyze the scoring Test dataset. As the listed config file pertains to a non-page level prediction model, we find the `noPage/noSegment` subDirectory in our required path
