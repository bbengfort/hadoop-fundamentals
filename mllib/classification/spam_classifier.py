import sys

from pyspark import SparkConf, SparkContext

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD


##########################################################################
# Main
##########################################################################

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Incorrect number of arguments, correct usage: spam_classifier.py [spamfile] [hamfile]"
        sys.exit(-1)

    # Configure Spark
    conf = SparkConf().setMaster("local") \
                      .setAppName("Spam Classifier") \
                      .set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    spam_file = sys.argv[1]
    ham_file = sys.argv[2]

    spam = sc.textFile(spam_file)
    ham = sc.textFile(ham_file)

    tf = HashingTF(numFeatures=10000)

    spam_features = spam.map(lambda email: tf.transform(email.split(" ")))
    ham_features = ham.map(lambda email: tf.transform(email.split(" ")))

    positive_examples = spam_features.map(lambda features: LabeledPoint(1, features))
    negative_examples = ham_features.map(lambda features: LabeledPoint(0, features))
    training = positive_examples.union(negative_examples)
    training.cache()

    model = LogisticRegressionWithSGD.train(training)

    # Create test data and test the model
    positive_test = tf.transform("Guaranteed to Lose 20 lbs in 10 days Try FREE!".split(" "))
    negative_test = tf.transform("Hi Mom, I'm learning all about Hadoop and Spark!".split(" "))

    print "Prediction for positive test example: %g" % model.predict(positive_test)
    print "Prediction for negative test example: %g" % model.predict(negative_test)
