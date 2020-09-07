package com.tfs.flashml.util.conf

/**
  * Single class containing all the constants used in FlashML.
  *
  * @since 17/11/16.
  */
object FlashMLConstants
{
    // spark configuration
    val CONTEXT = "flashml.context"

    val PACKAGE_LOG_LEVEL = "project.logLevel"

    val FLASHML_PROJECT_ID = "project.id"
    val FLASHML_JOB_ID = "job.id"
    val FLASHML_EXPERIMENT_ID = "experiment.id"
    val FLASHML_MODEL_ID = "model.id"


    val RUNNING = "RUNNING"
    val COMPLETED = "COMPLETED"

    // Vertica Related Configurations
    val VERTICA_HOST_URL = "vertica.host.url"
    val VERTICA_JDBC_DRIVER = "vertica.jdbc.driver"
    val VERTICA_USER_NAME = "vertica.user.name"
    val VERTICA_USER_PASSWORD = "vertica.user.password"

    // Apollo server url
    val APOLLO_API_URL = "apollo.api.url"

    val EXPERIMENT_TYPE = "experiment.type"
    val EXPERIMENT_RETRAIN_ID = "experiment.retrainId"

    val EXPERIMENT_TYPE_MODEL = "model"
    val EXPERIMENT_TYPE_PREDICT = "predict"
    val EXPERIMENT_TYPE_MONITORING = "monitoring"
    val EXPERIMENT_MODELING_METHOD = "experiment.modelingMethod"
    val EXPERIMENT_NUMBER_OF_PAGES = "experiment.pageLevel.numberOfPages"
    val EXPERIMENT_PARALLELISM = "experiment.parallelism"

    val EXPERIMENT_MODELING_METHOD_SINGLE_INTENT = "single_intent"
    val EXPERIMENT_MODELING_METHOD_MULTI_INTENT = "multi_intent"
    val PRIMARY_KEY = "experiment.primaryKey"
    val SAMPLE_SPLIT = "experiment.sample.split"
    val QA_DATA_GENERATION = "qadatageneration"
    val SAMPLE_CONDITION = "experiment.sample.condition"
    val SAMPLING_TYPE = "experiment.sample.type"
    val SAMPLING_TYPE_RANDOM = "random"
    val SAMPLING_TYPE_CONDITIONAL = "conditional"
    val SAMPLING_TYPE_STRATIFIED = "stratified"
    val MINIMUM_CLASS_SUPPORT = "experiment.sample.stratified.minimumClassSupport"
    val OTHER_CLASS_VALUE = "experiment.sample.stratified.otherClassValue"
    val MINIMUM_CLASS_SUPPORT_REQUIRED = "experiment.sample.stratified.minimumClassSupportRequired"
    val RESPONSE_VARIABLE = "experiment.responseVariable"
    val PAGE_VARIABLE = "experiment.pageVariable"
    val DATE_VARIABLE = "experiment.dateVariable"
    val UPLIFT_VARIABLE = "experiment.uplift.treatmentVariable"
    val NUMERIC_VARIABLES = "experiment.numericVariables"
    val CATEGORICAL_VARIABLES = "experiment.categoricalVariables"
    val TEXT_VARIABLES = "experiment.textVariables"
    val ADDITIONAL_VARIABLES = "experiment.additionalVariables"
    val CUMULATIVE_SESSION_TIME = "experiment.cumulativeSessionTime"
    val PREDICTION_OUTPUT_VARIABLES = "rawPrediction,probability,prediction"
    val RANDOM_VARIABLE = "experiment.randomVariable"
    val RANDOM_VARIABLE_COLUMN_NAME = "sparkRandomVariable"
    val RANDOM_NUMBER_GENEARATOR_VARIABLE = "experiment.randomNumberGeneratorVariable"

    //variables scope
    val VARIABLES_SCOPE = "experiment.variables.scope"
    val SCOPE_PARAMETER_NO_PAGE = "nopage"
    val SCOPE_PARAMETER_PER_PAGE = "perpage"
    val SCOPE_PARAMETER_ALL_PAGE = "allpage"

    val VARIABLES_TEXT_COL = "experiment.variables.text"
    val VARIABLES_NUMERICAL_COL = "experiment.variables.numerical"
    val VARIABLES_CATEGORICAL_COL = "experiment.variables.categorical"

    // For top intents info in Multi-Intent
    val TOP_K_INTENTS_COLUMN_NAME = "experiment.multiIntent.topIntentsColumn"
    val TOP_K_INTENTS_K_VALUE = "experiment.multiIntent.maxTopIntents"

    // preprocessing
    val EXPERIMENT_PREPROCESSING_STEPS = "experiment.preprocessing.steps"
    val PREPROCESSING_SCOPE = "experiment.preprocessing.scope"
    val PREPROCESSING_SCOPE_NONE = "none"

    val INPUT_VARIABLE = "inputVariable"
    val OUTPUT_VARIABLE = "outputVariable"
    val INPUT_PARAMETER = "parameter"

    // preprocessing functions
    val TOKENIZER = "tokenizer"
    val NULL_CHECK = "nullcheck"
    val CUSTOM_DELIMITER = "~SP~"
    val STOPWORDS = "stopwords"
    val CONTRACTIONS_REPLACEMENT = "contractions_replacement"
    val N_GRAM = "ngram"
    val SENTENCE_MARKER = "sentence_marker"
    val WORD_CLASSES_REPLACEMENT = "word_classes_replacement"
    val STEMMING = "stemming"
    val LEMMATIZE = "lemma"
    val SKIP_GRAM = "skip_gram"
    val REGEX_REPLACEMENT = "regex_replacement"
    val REGEX_REMOVAL = "regex_removal"
    val CASE_NORMALIZATON = "case_normalization"
    val PREPROCESSING_NEW_COL_OP_NAME = "outputVariable"
    val PREPROCESSING_TYPE = "type"
    val PREPROCESSING_METHOD_PARAMETER = "parameter"
    val PREPROCESSING_TRANSFORMATIONS = "transformations"

    // Data source configuration
    val INPUT_PATH = "project.data.location"
    val FILTER_CONDITION = "experiment.customFilter"
    val POST_PREDICT_FILTER = "experiment.postPredictFilter"
    val SCHEMA_FILE = "project.data.schema.file"

    // Feature generation
    val EXPERIMENT_FEATURE_GENERATION = "experiment.featuregeneration"
    val EXPT_FEATURE_GENERATION_SCOPE = "experiment.featuregeneration.scope"
    val EXPERIMENT_FEATURE_GENERATION_GRAMS = "experiment.featuregeneration.grams"
    val EXPERIMENT_FEATURE_GENERATION_BINNING: String = "experiment.featuregeneration.binning"
    val FEATURE_GENERATION_BINNING: String = "binning"
    val FEATURE_GENERATION_GRAMS = "grams"
    val BINNING_TYPE_EQUIDISTANT = "equidistant"
    val BINNING_TYPE_EQUIAREA = "equiarea"
    val BINNING_TYPE_INTERVALS = "intervals"
    val BINNING_TYPE = "type"

    // Feature engineering
    val HASHING_TF = "hashingtf"
    val COUNT_VECTORIZER = "count_vectorizer"
    val WORD2VEC = "word2vec"
    val TF_IDF = "tfidf"
    val CATEGORICAL_VARIABLES_VECTORIZATION_METHOD = "experiment.vectorization.categorical.method"
    val EXPERIMENT_TEXT_VECTORIZATION_SCOPE = "experiment.vectorization.text.scope"
    val EXPERIMENT_TEXT_VECTORIZATION_STEPS = "experiment.vectorization.text.steps"
    val SLOTS_CATEGORICAL_VARIABLES = "experiment.vectorization.categorical.slots"
    val VECTORIZATION_TEXT_METHOD = "method"
    val VECTORIZATION_TEXT_SLOT_SIZE = "slots"

    val CATEGORICAL_ARRAY = "categorical_array"
    val HASH_OUTPUT_COLUMN_SUFFIX = "_htf"
    val COUNT_VECTORIZER_OUTPUT_COLUMN_SUFFIX = "_count_vectorizer"

    // Experiment configuration
    val FEATURES = "features"
    val INDEXED = "_indexed"
    val STRINGIFIED = "_stringified"
    val PREDICTION = "prediction"
    val PREDICTION_LABEL = "prediction_label"
    val ALGORITHM = "experiment.algorithm.type"
    val BUILD_TYPE = "experiment.algorithm.build.type"

    val BUILD_TYPE_MULTINOMIAL = "multinomial"
    val BUILD_TYPE_BINOMIAL = "binomial"
    val BUILD_TYPE_OVR = "ovr"

    val FORMATTER = "formatter"
    val SVM_FORMATTER = "svmlight"

    // Algorithms
    val LOGISTIC_REGRESSION = "logistic_regression"
    val NAIVE_BAYES = "naive_bayes"
    val SVM = "svm"
    val RANDOM_FOREST = "random_forest"
    val GRADIENT_BOOSTED_TREES = "gradient_boosted_trees"
    val DECISION_TREES = "decision_trees"
    val MULTILAYER_PERCEPTRON = "multilayer_perceptron"

    // Algortihm related configurations
    val LR_REGULARISATION = "experiment.algorithm.logistic.regparam"
    val LR_ITERATIONS = "experiment.algorithm.logistic.maxiter"
    val LR_ELASTIC_NET = "experiment.algorithm.logistic.elasticnetparam"
    val LR_STANDARDIZATION = "experiment.algorithm.logistic.standardization"

    // SVM related configuration
    val SVM_REGULARISATION = "experiment.algorithm.svm.regparam"
    val SVM_ITERATIONS = "experiment.algorithm.svm.maxiter"
    val SVM_STANDARDIZATION = "experiment.algorithm.svm.standardization"
    val SVM_PLATT_SCALING_ENABLED = "experiment.algorithm.svm.plattScalingEnabled"

    // Random Forest related configuration
    val RF_NUMBER_OF_TREES = "experiment.algorithm.random_forest.numberOfTrees"
    val RF_IMPURITY = "experiment.algorithm.random_forest.impurity"
    val RF_MAXDEPTH = "experiment.algorithm.random_forest.maxDepth"
    val RF_FEATURESUBSETSTRATEGY = "experiment.algorithm.random_forest.featureSubsetStrategy"

    // Gradient Boosted Trees related configurations
    val GBT_MAX_ITER = "experiment.algorithm.gradient_boosted_trees.maxIterations"
    val GBT_FEATURESUBSETSTRATEGY = "experiment.algorithm.gradient_boosted_trees.featureSubsetStrategy"
    val GBT_MAX_DEPTH = "experiment.algorithm.gradient_boosted_trees.maxDepth"
    val GBT_IMPURITY = "experiment.algorithm.gradient_boosted_trees.impurity"

    // Decision Trees related user-entered config params
    val DT_IMPURITY = "experiment.algorithm.decision_trees.impurity"
    val DT_MAX_DEPTH = "experiment.algorithm.decision_trees.maxDepth"
    val DT_CACHE_NODE_ID_BOOL = "experiment.algorithm.decision_trees.cacheNodeIDs"
    val DT_MAX_BINS = "experiment.algorithm.decision_trees.maxBins"

    // Naive Bayes related user-entered config params
    val NB_MODEL_TYPE = "experiment.algorithm.naive_bayes.modeltype"
    val NB_MODEL_TYPE_BERNOULLI = "bernoulli"
    val NB_MODEL_TYPE_MULTINOMIAL = "multinomial"
    val NB_SMOOTHING = "experiment.algorithm.naive_bayes.smoothing"

    // Multi Layer Perceptron user-entered config params
    val MLP_BLOCK_SIZE = "experiment.algorithm.multilayer_perceptron.blockSize"
    val MLP_MAX_ITERATIONS = "experiment.algorithm.multilayer_perceptron.maxIter"
    val MLP_INTERMEDIATE_LAYERS = "experiment.algorithm.multilayer_perceptron.intermediateLayers"

    val CROSS_VALIDATION = "experiment.cv.folds"
    val HYPER_PARAM_OP = "experiment.hyperparamop"
    val CV_PREDICT_SAVEPOINT = "experiment.cv.predictSavepoint"
    val CV_EVAL_METRIC = "experiment.cv.metric"
    val CV_EVAL_METRIC_DEFAULT = "weightedPrecision"

    // Hyperband related
    // TODO include these in comments
    val HYPERBAND_ITERATIONS = "experiment.hyperband.iterations"
    val HYPERBAND_ITER_MULTIPLIER = "experiment.hyperband.iterMultiplier"
    val HYPERBAND_ETA = "experiment.hyperband.eta"
    val HYPERBAND_TRAIN_SIZE = "experiment.hyperband.trainSize"

    // HDFS configurations
    val NAME_NODE_URI = "hdfs.nameNode.uri"

    val HIVE_THRIFT_URL = "hive.thrift.url"

    // Folder Structure
    val ROOT_DIRECTORY = "flashml.rootDirectory"

    // Simulation Inputs
    val N_PAGES = "experiment.customMetrics.npages"
    val CUSTOM_THRESHOLDS = "experiment.customMetrics.thresholds"
    val TOP_VARIABLE = "experiment.customMetrics.topVariable"
    val CUSTOM_METRICS_TYPE = "experiment.customMetrics.type"
    val TOP_THRESHOLDS = "experiment.customMetrics.topList"
    val PROB_ONLY_CUSTOM_METRICS = "customProbBased"
    val PROB_AND_TOP_CUSTOM_METRICS = "customProbAndTopBased"

    // Publish Inputs
    val PUBLISH_FORMAT = "experiment.publish.format"
    val PUBLISH_IS_HOTLEAD_MODEL = "experiment.publish.isHotLeadModel"
    val PUBLISH_PAGES = "experiment.publish.online.pages"
    val PUBLISH_THRESHOLDS = "experiment.publish.thresholds"
    val PUBLISH_TOPTHRESHOLDS = "experiment.publish.topList"
    val PUBLISH_PRECISION = "experiment.publish.precision"

    // Publish formats
    val PUBLISH_FORMAT_JS = "js"
    val PUBLISH_FORMAT_MLEAP = "mleap"
    val PUBLISH_FORMAT_SPARK = "spark"

    //QA Inputs
    val QA_DATAPOINTS = "experiment.qa.dataPoints"
    val QA_FORMAT = "experiment.qa.format"

    // Pipeline Steps
    val PIPELINE_STEPS = "pipeline.steps"
    val DIRECTORY_CREATOR = "directorycreator"
    val DATAREADER = "datareader"
    val PREPROCESSING = "preprocessing"
    val FEATURE_GENERATION = "featuregeneration"
    val VECTORIZATION = "vectorization"
    val SAMPLING = "sampling"
    // TODO: Remove the step name as "modelling" from future releases. That involves changing the step name in the config files, among other places.
    val MODELLING = "modelling"
    val MODEL_TRAINING = "modelTraining"
    val SCORING = "scoring"
    val STANDARD_METRICS = "standardmetrics"
    val CUSTOM_METRICS = "custommetrics"
    val PUBLISH = "publish"

    val SAVEPOINTING_REQUIRED = "savePointing.required"

    val MINORITY_CLASS_THRESHOLD = 0.002
    val MINORITY_CLASS_PERCENT = "experiment.data.positivePercent"
    val MAX8BITHEXDOUBLE = 4.294967295E9d
    val SEED = 20

    // Modelling method
    val PAGE_LEVEL = "page_level"
    val UPLIFT = "uplift"

    val CONFIG_CHECKS_LIST = "/configChecks.json"

    // Default value for parallelism
    val parallelism = 3

    val probabilitySupportedAlgorithms = Set(
        FlashMLConstants.DECISION_TREES,
        FlashMLConstants.GRADIENT_BOOSTED_TREES,
        FlashMLConstants.NAIVE_BAYES,
        FlashMLConstants.MULTILAYER_PERCEPTRON,
        FlashMLConstants.RANDOM_FOREST,
        FlashMLConstants.LOGISTIC_REGRESSION
    )
}