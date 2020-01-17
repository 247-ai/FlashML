# The beginner's guide to FlashML

This guide aims to help users build a prediction model. The steps and the methods listed here are not exhaustive, rather they serve as a simple stepwise guide to quickly build a Natural Language based intent prediction model.

FlashML now supports a scope-wise declaration of variables and config parameters for multiple pipeline steps, helping users to quickly build multi-page level models without repeating config. options repeatedly for all the pages. Support for scope processing has been provided in declaring columns as well as the `preprocessing`, `feature generation` and `vectorization` steps. The scope parameters accepted are:
<ul>
 <li>NoPage</li>
 <li>AllPage</li>
 <li>PerPage</li>
</ul>

For a detail account of all supported options/ features of scope processing in FlashML, users may refer to the
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

##### Quick Links
-   [The Pipeline Steps](#the-pipeline-steps)
-   [Project Structure](#project-structure)
    -   [Data Reader](#data-reader)
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

## The Pipeline Steps
The pipeline steps are to be listed in the configuration file. The submitted Spark job reads the parameters from the submitted configuration file and executes the FlashML jar file.

For each of the steps we shall list the functioning of the prediction model building process with sample configuration JSON snippets.
Alternate methods and additional customizations are supported in loading the data, all of which are listed in the <code>FlashML User Guide</code> here at the
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

### Project Structure
The updated FlashML project structure allows the user to run multiple jobs for the same Project. Loading the input data and supporting parameters is now easier when running jobs at the same project level.

<ol>
<li><h4 id="data-reader">Data Reader</h4></li>

Before submitting the Spark job and starting the prediction model building process, users are required to load data onto the Spark cluster environment.
For this case, we load a Hive table. It is assumed that the user has already loaded the data onto Hive.
Alternate methods and additional customizations are supported in loading the data, all of which are listed in the `FlashML User Guide`.

##### Sample Config 1. Loading data from a Hive Table

```
"project":{
    "data":{
        "location":{
            "source":"hive://FLASHML.NL_TEST_DATA"
        }
    }
}

```

<br />
<li><h4 id="sampling">Sampling</h4></li>
FlashML supports multiple options of sampling the data, in this case the user requires the data to be split in the ratio 70:30 for the train and test datasets.

##### Config. Snippet 2. Random sampling of data
```
"experiment": {
    "sample": {
        "type": "random",
        "split": [
            70,
            30
            ]
        }
    }
```

<li><h4 id="preprocessing">Preprocessing</h4></li>

Users can choose from a range of FlashML supported preprocessing techniques that include :

<ul>
 <li><code>Stopwords</code> </li>
 <li><code>Tokenizer</code> </li>
 <li><code>Stemming</code> </li>
 <li><code>Regular Expression Replacement</code></li>
 <li><code>Case Normalization</code></li>
 <li><code>Word Classes Replacement</code></li>
 <li><code>Regular Expession Removal</code></li>
 <li><code>Sentence Marker</code></li>
 <li><code>Contractions Replacement</code></li>
 <li><code>Null Check</code></li>
</ul>
<br />

The configuration sample snippet below first replaces all `Null` values with empty strings, then tokenizes the rows with the specified delimiter, then excludes all occurrences of the `stopwords` in the dataset and stores the result in a newly created column named `filteredWords`.

##### Config. Snippet 3. Preprocessing on tokenized data
```
{
  "experiment": {
    "preprocessing": {
      "scope":"noPage",
      "steps": [
          {
            "inputVariable": "chatContent",
            "outputVariable":"filteredWords",
            "transformations": [
              {
                "type":"nullCheck",
                "parameter":""
              },
              {
                "type":"tokenizer",
                "parameter":"/"
              },
              {
                "type":"stopwords",
                "parameter": [
                  "my",
                  "their",
                  "the"
                ]
              }
            ]
          }
      ]
    }
  }
}
```

For more information on how FlashML supports preprocessing techniques, please refer to the
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

<br />
<li><h4 id="feature-generation">Feature Generation</h4></li>

Users can specify page wise feature generation text transformations on the preprocessed datapoints, by choosing from the FlashML supported feature generation options:

<ul>
<li>Ngram</li>
<li>Skip Gram</li>
</ul>

From the config. snippet below, it is clear that sequences of 2 and 3 sized `Ngram` blocks are obtained.
The user must list the output variable column name, and ensure it is mentioned in the subsequent steps.

##### Config. Snippet 4. Feature Generation on preprocessed data
```
{
  "experiment": {
    "featuregeneration": {
      "scope":"noPage",
      "steps": [
          {
            "inputVariable": "filteredWords",
            "outputVariable": "gramWords",
            "transformations": [
              {
                "ngram": [2,3]
              }
            ]
          }
      ]
    }
  }
}
```

Additionally FlashML also supports <code>Binning</code> of numerical variables at a pagewise level.
The types of binning supported are:
<ul>
 <li>Custom Intervals</li>
    The binning intervals can be directly entered in this option(in ascending order).
 <li>Equi - Distant Intervals</li>
    Users can enter the required number of buckets and the bins generated will have boundary intervals equidistant.
 <li>Equi - area Intervals</li>
    Users can enter the required number of buckets and the bins generated will have roughly the same number of values.
</ul>

From the sample config. snippet listed below, the binning intervals are directly entered.
The user is required to ensure values are entered in ascending order.
##### Config. Snippet 5. Binning on numerical data

```
{
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
}


```

For more information on how FlashML supports feature generation, users may refer to the
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

For information on how Spark supports extraction and transformation of features, please refer to the
[Spark Documentation](https://spark.apache.org/docs/latest/ml-features.html#bucketizer)

<li>
<h4 id="vectorization">Vectorization</h4>
</li>

Users can specify a page wise sequence of vectorization options on the datapoints, by choosing from among the following FlashML supported options to generate term frequency vectors:
<br />
<ul>
 <li><code>HashingTF</code></li>
 <li><code>count_vectorizer</code></li>
 <li><code>word2vec</code></li>
 <li><code>TfIDF</code></li>
</ul>

<br />

A sample snippet is listed here for reference. For more information, please refer to the
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

##### Config. Snippet 6. Vectorization of Features generated
```
{
  "experiment": {
    "vectorization": {
      "text": {
        "scope":"noPage",
        "steps":
          [
            {
              "inputVariable": "gramWords",
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

<br />
<li>
<h4 id="modeling">Modeling</h4>
</li>

Users can specify a number of parameters on how the prediction model is to be built. Some parameters include: the algorithm, modeling methods, build type, number of pages, and parameters for the different Machine Learning algorithms. A sample snippet is listed here for reference, more information is available on the <code>FlashML Comprehensive Guide</code>.

##### Config. Snippet 7. Modeling Method block
```
{
  "experiment": {
    "modelingMethod": [
      "single_intent",
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
      "numberOfPages": 1
    }
  }
}
```

FlashML supports the computation of a k-fold Cross Validation, as well as saving the required prediction datasets for each of the 'k' folds.

##### Config. Snippet 8. K-fold Cross Validation
```
{
  "experiment": {
    "cv": {
      "folds": 3,
      "predictSavepoint": true
    }
  }
}
```

For more information on the same, Users may refer to the the user guide here at [Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)

<br />
<li><h4 id="scoring">Scoring</h4></li>

The model built with the specified parameters is run on the input datasets. Depending on the savePointing.required boolean parameter entered by the user, the prediction datapoints are saved on the HDFS project directory.

##### Config. Snippet 8. Scoring parameters applied
```
{
  "experiment": {
    "response_variable": "intent",
    "postPredictFilter": []
  },
  "savePointing": {
    "Required": true
  }
}
```

<li><h4 id="standard metrics">Standard Metrics</h4></li>

No parameters are required from the user here. It is expected that the user has filled the parameter `experiment.rootDirectory` as the metrics are saved in the project directory on HDFS, at the location `$"rootDirectory"`+`"/metrics/standardMetrics/stdMetrics.json"`

<li>
<h4 id="custom metrics">Custom Metrics</h4>
</li>

Users may refer to the the user guide here at
[Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)


<li>
<h4 id="publish">Publish</h4>
</li>

Users can generate execution-ready scripts using the parameter `experiment.publish`
A sample snippet is listed here for reference.

##### Config. Snippet 9. Sample snippet for generating the deployment-ready script file.

```
"publish":{
      "thresholds":[0.2],
      "format":"js",
      "online":{
        "pages":[1]
      }
    }
```

Users may refer to the the comprehensive user guide here at [Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)
</ul>


<li><h4 id="qa data generation">QA Data Generation</h4></li>

Users may refer to the the user guide here at [Comprehensive FlashML User Guide](The%20comprehensive%20FlashML%20user%20guide.MD)
<br />

</ol>