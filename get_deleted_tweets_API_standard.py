#call api sends a bulk on 100 tweet ids and return those deleted , write to file
'''
    Last updated by r.t 2/12/14 This is rv.2
    test to git 
'''
import time
import json
import urllib2
import oauth2
from pymongo import MongoClient
 
deleted_file=open("deleted_ids.txt","a")


def get_del_ids(input_ids,output_ids):
    return set(input_ids).difference(set(output_ids))


def write_del_file(deleted_ids):
    for ID in deleted_ids:
        deleted_file.write(str(ID)+'\n')
        
def get_id_api(db,APIUrl,pool_oauth,globalCounter):
    size_deleted=0
    input_ids=[]
    output_ids=[]
    tweet_ids=""
    tweet_counter=1
    pool_ind=0
    for post in db.posts.find():#.skip(245001):  #last count79447
        #concatinate 100 ids in a string format with a 54563999e0d4c11d225f3d9f"," in between them then provide it to the API
        globalCounter+=1
        if "id" in post:
            id=int(post["id"])
            #print id
            if tweet_counter<100:
                #print tweet_counter
                tweet_ids=tweet_ids+str(id)+","
                input_ids.append(id)
                tweet_counter +=1
                
                
            
            else:

                tweet_ids=tweet_ids+str(id)
                print tweet_ids
                input_ids.append(id)
                print "Calling API on tweets...", tweet_counter
                url = APIUrl
                try:
                    
                    params = {"oauth_version":"1.0","oauth_nonce": oauth2.generate_nonce(),"oauth_timestamp":int(time.time())}
                    params["oauth_consumer_key"] = (pool_oauth[pool_ind][0]).key
                    params["oauth_token"] = (pool_oauth[pool_ind][1]).key
                    params["id"] =tweet_ids #list of 100 tweets
                    params["include_entities"] ="" #optional
                    params["trim_user"] =""#optional
                    params["map"] = ""#optional
                 
                    req = oauth2.Request(method="GET", url=url, parameters=params)
                    signature_method = oauth2.SignatureMethod_HMAC_SHA1()# HMAC, twitter use sha-1 160 bit encryption
                    req.sign_request(signature_method, (pool_oauth[pool_ind][0]), (pool_oauth[pool_ind][1]))
                    headers = req.to_header()
                    url = req.to_url()
                
                 
                    response = urllib2.Request(url)
                    data = json.load(urllib2.urlopen(response))#format results as json
                    #find ids that are not in the output_ids
                    for i in range(0, len(data)):
                        if "id" in  data[i]:
                            output_ids.append(int(data[i]["id"]))
                    print input_ids
                    print output_ids
                    print "input size",len(input_ids)
                    print "output size",len(output_ids)
                    deleted_ids=get_del_ids(input_ids,output_ids)
                    size_deleted= size_deleted + len(deleted_ids)
                    print deleted_ids
                    write_del_file(deleted_ids)
                    
                    tweet_counter=1
                    tweet_ids=""
                    input_ids=[]
                    output_ids=[]
                    
                    print "global counter:", globalCounter
                    
                    
                    
                except Exception, ex:
                    print 'Exception: '+str(ex)
                    
                finally:
                    time.sleep(3)
                    pool_ind+=1
                    pool_ind= pool_ind % 4

            
    if tweet_counter>1:
        print tweet_ids
        print "counter", tweet_counter
        print "global counter:", globalCounter
        url = APIUrl
        params = {"oauth_version":"1.0","oauth_nonce": oauth2.generate_nonce(),"oauth_timestamp":int(time.time())}
        params["oauth_consumer_key"] = (pool_oauth[pool_ind][0]).key
        params["oauth_token"] = (pool_oauth[pool_ind][1]).key
        params["id"] =tweet_ids #list of 100 tweets
        params["include_entities"] ="" #optional
        params["trim_user"] =""#optional
        params["map"] = ""#optional
    
        req = oauth2.Request(method="GET", url=url, parameters=params)
        signature_method = oauth2.SignatureMethod_HMAC_SHA1()# HMAC, twitter use sha-1 160 bit encryption
        req.sign_request(signature_method, pool_oauth[pool_ind][0], pool_oauth[pool_ind][1])
        headers = req.to_header()
        url = req.to_url()

        response = urllib2.Request(url)
        data = json.load(urllib2.urlopen(response))#format results as json

        print 'length or record:', len(data)
        for i in range(0, len(data)):
            if "id" in  data[i]:
                output_ids.append(int(data[i]["id"]))
                print input_ids
                print output_ids
                print len(input_ids)
                print len(output_ids)
                deleted_ids=get_del_ids(input_ids,output_ids)
                size_deleted= size_deleted+ len(deleted_ids)
                write_del_file(deleted_ids)
    print "deleted posts ",size_deleted 
def main():
    
    globalCounter=0
    global size_deleted
    consumer1 = oauth2.Consumer(key="GTcvT0vIFw3MTm3Ft8N8IYnZS", secret="BBNXXGUYO0inoEQZFqqUhVLYR1A9eiQoY1CDVuBmNfBTQzQdXZ")
    token2 = oauth2.Token(key="2427867182-7Ffe8DDIry4x3ZCpJ969LpD8WtnWv3uP43QLaLn", secret="oXqwTaEfUa4okcDUFFU0gBYAVdx9Ra5CS8wNO3PUMQvno")
    
    consumer1 = oauth2.Consumer(key="OPBxPJgxtsmLHt6z33qc3fbyX", secret="2b6OqtF4yQIT5KttW4ahvUZWhczpGRWQnhqLsQQXegVjioxIM2")
    token1 = oauth2.Token(key="2427867182-pg3uTH2xv8L8dmZ2SbsPFgGhChZ1fcwqKNSC8kv", secret="ThFAV7rqXpPV6G6asTSouyTrtK4RuUzcKUONsuty4g4hH")
    
    
    consumer2 = oauth2.Consumer(key="Ytt3EhPJfjteJJlstaVP6KrdI", secret="HGqZOpTzbVCbpnebpNhimRJnJ8dnn4grKBM5tjvyl294jZjLWJ")
    token2 = oauth2.Token(key="2427867182-YXmUUMwUQJBdoL36ivUpMeTVEqGi4n0CQEjx7ep", secret="oePoVJFA4GCnNW2D1QkZ8Jyn2cZd4sUnhPRAAVWzrlh5v")
    
    consumer3 = oauth2.Consumer(key="GTcvT0vIFw3MTm3Ft8N8IYnZS", secret="BBNXXGUYO0inoEQZFqqUhVLYR1A9eiQoY1CDVuBmNfBTQzQdXZ")
    token3 = oauth2.Token(key="2427867182-7Ffe8DDIry4x3ZCpJ969LpD8WtnWv3uP43QLaLn", secret="oXqwTaEfUa4okcDUFFU0gBYAVdx9Ra5CS8wNO3PUMQvno")
    
    
    consumer4 = oauth2.Consumer(key="pw3D3MpehTRxLCHM0P9KQ", secret="aVnzA6gL3bhHBhNNludBeMlIsHLPC6GOZ3JjMGWJI")
    token4 = oauth2.Token(key="2182807500-XN76iiRxPSrPnHSdjNH8On2k2nmak5q8TOmLWIs", secret="Y2F9s6k8p7OSbWL1wl9YS4Azvml9hf9vbTm0zPd7eQfBV")
    
    pool_oauth=[(consumer1,token1),(consumer2,token2),(consumer3,token3),(consumer4,token4)]
    
    #params["oauth_consumer_key"] = consumer.key
    #params["oauth_token"] = token.key
    APIUrl='https://api.twitter.com/1.1/statuses/lookup.json'
    '''
    here goes some mongodb code
    '''
    #************thisis the database we are reading from************8
    client = MongoClient('localhost', 27017)
    db = client['combined_TurkishE_KnownUnk_NoWA']
    #collection=db['combined_TurkishE_KnownUnk_NoWA']
    


    get_id_api(db,APIUrl,pool_oauth,globalCounter)
    deleted_file.close()
    
    
    
    
if __name__=="__main__":
    main()
                
            
       

