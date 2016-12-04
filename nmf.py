# -*- coding: utf-8 -*-

# Created by Yun Zhou to generate preprocessed 
# tweets data for the lda learner program
from pymongo import MongoClient
import datetime
import pickle
import nltk.data
import sys
import subprocess
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF  
import os
reload(sys)  
sys.setdefaultencoding('utf8')

# figure out how data.load works
tokenizer = nltk.data.load('tokenizers/punkt/turkish.pickle')
stopwords = ['``', ';', "'", '+' ,'(',')',"''", ':','http','#','@','.','..','...',',','?','!','-','https','...','a','acaba', 'altm\xc4\xb1\xc5\x9f', 'alt\xc4\xb1', 'ama', 'ancak', 'arada', 'asl\xc4\xb1nda', 'ayr\xc4\xb1ca', 'bana', 'baz\xc4\xb1', 'belki', 'ben', 'benden', 'beni', 'benim', 'beri', 'be\xc5\x9f', 'bile', 'bin', 'bir', 'bir\xc3\xa7ok', 'biri', 'birka\xc3\xa7', 'birkez', 'bir\xc5\x9fey', 'bir\xc5\x9feyi', 'biz', 'bize', 'bizden', 'bizi', 'bizim', 'b\xc3\xb6yle', 'b\xc3\xb6ylece', 'bu', 'buna', 'bunda', 'bundan', 'bunlar', 'bunlar\xc4\xb1', 'bunlar\xc4\xb1n', 'bunu', 'bunun', 'burada', '\xc3\xa7ok', '\xc3\xa7\xc3\xbcnk\xc3\xbc', 'da', 'daha', 'dahi', 'de', 'defa', 'de\xc4\x9fil', 'di\xc4\x9fer', 'diye', 'doksan', 'dokuz', 'dolay\xc4\xb1', 'dolay\xc4\xb1s\xc4\xb1yla', 'd\xc3\xb6rt', 'edecek', 'eden', 'ederek', 'edilecek', 'ediliyor', 'edilmesi', 'ediyor', 'e\xc4\x9fer', 'elli', 'en', 'etmesi', 'etti', 'etti\xc4\x9fi', 'etti\xc4\x9fini', 'gibi', 'g\xc3\xb6re', 'halen', 'hangi', 'hatta', 'hem', 'hen\xc3\xbcz', 'hep', 'hepsi', 'her', 'herhangi', 'herkesin', 'hi\xc3\xa7', 'hi\xc3\xa7bir', 'i\xc3\xa7in', 'iki', 'ile', 'ilgili', 'ise', 'i\xc5\x9fte', 'itibaren', 'itibariyle', 'kadar', 'kar\xc5\x9f\xc4\xb1n', 'katrilyon', 'kendi', 'kendilerine', 'kendini', 'kendisi', 'kendisine', 'kendisini', 'kez', 'ki', 'kim', 'kimden', 'kime', 'kimi', 'kimse', 'k\xc4\xb1rk', 'milyar', 'milyon', 'mu', 'm\xc3\xbc', 'm\xc4\xb1', 'nas\xc4\xb1l', 'ne', 'neden', 'nedenle', 'nerde', 'nerede', 'nereye', 'niye', 'ni\xc3\xa7in', 'o', 'olan', 'olarak', 'oldu', 'oldu\xc4\x9fu', 'oldu\xc4\x9funu', 'olduklar\xc4\xb1n\xc4\xb1', 'olmad\xc4\xb1', 'olmad\xc4\xb1\xc4\x9f\xc4\xb1', 'olmak', 'olmas\xc4\xb1', 'olmayan', 'olmaz', 'olsa', 'olsun', 'olup', 'olur', 'olursa', 'oluyor', 'on', 'ona', 'ondan', 'onlar', 'onlardan', 'onlar\xc4\xb1', 'onlar\xc4\xb1n', 'onu', 'onun', 'otuz', 'oysa', '\xc3\xb6yle', 'pek', 'ra\xc4\x9fmen', 'sadece', 'sanki', 'sekiz', 'seksen', 'sen', 'senden', 'seni', 'senin', 'siz', 'sizden', 'sizi', 'sizin', '\xc5\x9fey', '\xc5\x9feyden', '\xc5\x9feyi', '\xc5\x9feyler', '\xc5\x9f\xc3\xb6yle', '\xc5\x9fu', '\xc5\x9funa', '\xc5\x9funda', '\xc5\x9fundan', '\xc5\x9funlar\xc4\xb1', '\xc5\x9funu', 'taraf\xc4\xb1ndan', 'trilyon', 't\xc3\xbcm', '\xc3\xbc\xc3\xa7', '\xc3\xbczere', 'var', 'vard\xc4\xb1', 've', 'veya', 'ya', 'yani', 'yapacak', 'yap\xc4\xb1lan', 'yap\xc4\xb1lmas\xc4\xb1', 'yap\xc4\xb1yor', 'yapmak', 'yapt\xc4\xb1', 'yapt\xc4\xb1\xc4\x9f\xc4\xb1', 'yapt\xc4\xb1\xc4\x9f\xc4\xb1n\xc4\xb1', 'yapt\xc4\xb1klar\xc4\xb1', 'yedi', 'yerine', 'yetmi\xc5\x9f', 'yine', 'yirmi', 'yoksa', 'y\xc3\xbcz', 'zaten']
# withheld = [1512118374, 1528527474, 2775606467, 2776783461, 323353537, 111437703, 2872713657, 2961800735, 516578419, 2567892499, 201035925, 2999695853, 1927671576, 2777554767, 2938117259, 2204006713, 835529077, 144910421, 2436878456, 536664198, 2811033792, 2230165195, 1587735252, 2765416702, 2354700919, 210913570, 1668569444, 2696366440, 2180309453, 2560901623, 2257902144, 2827063876, 1860629066, 2306253432, 1604469463, 1074928730, 2340758401, 2597972893, 738855900, 2282587194, 240055365, 536366335, 2317733082, 897072415, 566441316, 1084267908, 64050823]
withheld = [1512118374, 1528527474, 2775606467, 2776783461, 323353537, 111437703, 2872713657, 2961800735, 516578419, 2567892499, 201035925, 2999695853, 1927671576, 2777554767, 2938117259, 2204006713, 835529077, 144910421, 2436878456, 536664198, 2811033792, 2230165195, 1587735252, 2765416702, 2354700919, 210913570, 1668569444, 2696366440, 2180309453, 2560901623, 2257902144, 2827063876, 1860629066, 2306253432, 1604469463, 1074928730, 2340758401, 2597972893, 738855900, 2282587194, 240055365, 536366335, 2317733082, 897072415, 566441316, 1084267908, 64050823]


class Tweet:
    def __init__(self, tweet_id, text, hashtags, created_at, domains):
        self.tweet_id = tweet_id
        self.text = text
        self.hashtags = hashtags
        self.created_at = created_at
        self.domains = domains

    def __str__(self):
        return "Content: %s \nCreated: %s" % (self.get_content(), self.created_at)


# low = datetime.datetime.strptime("2014-10-05", "%Y-%m-%d")
# high = datetime.datetime.strptime("2014-10-25", "%Y-%m-%d")


def checktime(tweet, start, end):
    """
        return true if the tweet is created within a specified time frame, false otherwise
    """
    time = tweet[u'created_at']
    dt = datetime.datetime.strptime(time, '%a %b %d %H:%M:%S +0000 %Y')  # format the time
    start_time = datetime.datetime(2015, 6, start)
    end_time = datetime.datetime(2015, 6, end)
    if start_time <= dt < end_time:   
        return True
    return False


def checkoriginal(tweet):
    """
        return true if a tweet is original (or mention, reply) and not a duplicate text, false if it's a retweet
    """
    # duplicate set can become very large in size later on
    if u'retweeted_status' not in tweet and 'rt' not in tweet[u'text'].lower() and tweet[u'text'] not in duplicate:
        duplicate.add(tweet[u'text'])
        return True
    return False


def runmain(stem = False, filename = None, printout = True, start = 1, end = 2, sourcefile = None):
    """
        Arguments:
        stem: True if all tweets are stemmed first
        filename: the output file of the result
        printout: True if the reading process is printed to stdout
        start: date of the start day (inclusive, default month is 6, if you want to use another month, make change in checktime() function)
        end: date of the end day (exclusive, end date is NOT to be collected)
        sourcefile: True if the input text is already stored in a file (one tweet per line), False if the input text will
                    be read from mongoDB.
    """
  
    tweets = []
    if sourcefile:
        # read from temp_text.txt
        with open(sourcefile, "r") as lines:
            for line in lines:
                try:
                    tweets.append(tokenizer.tokenize(line)[0].encode("utf-8", errors='ignore'))  # ignore will just discard characters
                except:
                    continue
    else:
        counter = 0
        flag = True
        for tweet in collection.find():

            if u'text' in tweet and tweet[u'user'][u'id'] not in withheld:
                text = tweet[u'text']
                # if (text in duplicate):
                #    continue
                # else:
                #    duplicate.add(text)
                """ Uncomment if statement !!!"""
                # if checktime(tweet, start, end) and checkoriginal(tweet):
                if True:
                    tweets.append(tokenizer.tokenize(text)[0].encode("utf-8").replace("\n", " "))
                counter += 1
            if counter % 1000 == 0 and printout:
                print "Found %s tweets" % counter
            if counter > 10000:
                break

        print "Number of tweets collected: ", len(tweets)
        print "Start feature extraction"

        # get stemmed version
        if stem:
            tweets = stemming(tweets)

    dataset = tweets
    runNMF(tweets)


def stemming(tweets):
    # use absolute path
    fp = "temp_text" + str(time.time() * 1000) + ".txt"
    fp_out = "temp_text_out" + str(time.time() * 1000) + ".txt"
    fp = os.path.abspath(fp)
    fp_out = os.path.abspath(fp_out)
    with open(fp, "w") as text_file:
        for tweet in tweets:
            text_file.write(tweet)
            text_file.write('\n')
    print "temp file name is %s" % fp
    # call java program to convert all texts, WARNING: shell=True is a risky flag
    subprocess.call("/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home/bin/java  -Dfile.encoding=UTF-8 -classpath /Users/zc/Documents/twitter_research/TurkishElection/turkish-nlp-examples-master/target/classes:/Users/zc/.m2/repository/zemberek-nlp/core/0.9.0/core-0.9.0.jar:/Users/zc/.m2/repository/com/google/guava/guava/15.0/guava-15.0.jar:/Users/zc/.m2/repository/args4j/args4j/2.0.25/args4j-2.0.25.jar:/Users/zc/.m2/repository/zemberek-nlp/tokenization/0.9.0/tokenization-0.9.0.jar:/Users/zc/.m2/repository/antlr4-z3/antlr4-runtime/4.0.1z/antlr4-runtime-4.0.1z.jar:/Users/zc/.m2/repository/org/abego/treelayout/org.abego.treelayout.core/1.0.1/org.abego.treelayout.core-1.0.1.jar:/Users/zc/.m2/repository/zemberek-nlp/morphology/0.9.0/morphology-0.9.0.jar:/Users/zc/.m2/repository/zemberek-nlp/lm/0.9.0/lm-0.9.0.jar:/Users/zc/.m2/repository/org/simpleframework/simple-xml/2.5.3/simple-xml-2.5.3.jar:/Users/zc/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar:/Users/zc/.m2/repository/stax/stax/1.2.0/stax-1.2.0.jar:/Users/zc/.m2/repository/xpp3/xpp3/1.1.3.3/xpp3-1.1.3.3.jar:/Users/zc/.m2/repository/edu/berkeley/nlp/berkeleylm/1.1.2/berkeleylm-1.1.2.jar tokenization.TurkishTokenizationExample" + " " + fp + " " + fp_out, shell=True)        
    time.sleep(5)
    tweets = []
    # read from temp_text.txt
    with open(fp_out, "r") as lines:
        for line in lines:
            try:
                tweets.append(tokenizer.tokenize(line)[0].encode("utf-8"))
            except:
                continue

    os.remove(fp)
    os.remove(fp_out)
    return tweets



def runNMF(dataset, filename = None, n_features = 10000, n_topics = 10, n_top_words = 10):
    # n_features = n_features
    # n_topics = n_topics
    # n_top_words = n_top_words

    vectorizer = TfidfVectorizer(max_df=0.95, min_df=2, max_features=n_features,
                                 stop_words=stopwords)
    print type(dataset)
    print "Number of tweets collected (after preprocessing): ", len(dataset)
    
    tfidf = vectorizer.fit_transform(dataset)
    print "Fitting the NMF model with n_samples=%d and n_features=%d..." % (len(dataset), n_features)
    nmf = NMF(n_components=n_topics, random_state=1).fit(tfidf)

    feature_names = vectorizer.get_feature_names()
    if not filename:
        for topic_idx, topic in enumerate(nmf.components_):
                print "Topic #%d:" % topic_idx
                print " ".join([feature_names[i].encode("utf-8") for i in topic.argsort()[:-n_top_words - 1:-1]])
                print
    else:
        with open(filename, "w") as f:
            for topic_idx, topic in enumerate(nmf.components_):
                f.write("Topic #%d:" % topic_idx)
                f.write(" ".join([feature_names[i].encode("utf-8") for i in topic.argsort()[:-n_top_words - 1:-1]]))
                f.write('\n')
        print 'finish writing to the file'

if __name__ == '__main__':
    # Connect to the database
    client = MongoClient()
    db = client.geoTurkeyCrawlonlyElectionJune32015                              # choose a database
    collection = db.posts                    # choose a collection
    duplicate = set([])
    """
        READ documentation of runmain() and change arguments as you need.
    """
    print "this is main"
    runmain(stem = True, filename = "stemmed_topic_2015_6_7_all_tweets.txt", printout = True, start = 7, end = 8)  # 5-7, 9, 13-16, 25
    # stemmed_topic_2015_6_5_to_2015_6_7_all_tweets
    count = 0
    # for tweet in collection.find():
    #     #print tweet[u'withheld_in_countries'][0] == "BR"
    #     if "TR" in tweet[u'withheld_in_countries'][0] == "TR":
    #         count += 1
    # print count
    # for i in withheld:
    #     if i not in new:
    #         new.append(i)
    # print new
    # print len(new)

# Topic #0:
# diyarbakır rt hdp nin mitingine oy mitinginde işid yilmazgedik bombalı
# name of the city; rt; hdp; nin; rally; Islamic State of Iraq and the Levant; YilmazGedik is an account name; bomb

# Topic #1:
# türkiye akp chp mhp geneli 25 açılan sandık 12 16

# Topic #2:
# co lan mühürlü kulp bulundu nde lisesi yırtılmış sandığınasahipcık oylar
# related election fraud talking about damaged vote box

# Topic #3:
# demirtasdiyarbakırda amed jı gün yarın halkın amede doğacak umudu başka
# related to Diyarbakir kurd bombing

# Topic #4:
# su_sahinn bi geliyor hiç değil li öyle yok sorun tabi



