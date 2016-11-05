# -*- coding: utf-8 -*-


import time
import json
import urllib2
import oauth2
from pymongo import MongoClient
import pickle
client = MongoClient('localhost', 27017)
db = client["ActiveusersProfile_w_deleted_Tweets"]
unreachable=open("unreachable_users_from_API_lookup.txt","a")
def saveToDB(data):
        #Inserting into db        
        try:
            db.posts.insert(data)
        except Exception, ex:
            print 'Exception: '+str(ex)
        return True

def get_user_api(users,APIUrl,pool_oauth,globalCounter):
    size_deleted=0
    unreachable_user=[]
    active_users=[]
    user_id=""
    tweet_counter=0
    pool_ind=0
    for user in users:#.skip(245001):  #last count79447
        #concatinate 100 ids in a string format with a 54563999e0d4c11d225f3d9f"," in between them then provide it to the API
        globalCounter+=1
        
        user_id=str(int(user))
        #print idstr
            
        tweet_counter +=1

        print "Calling API on user lookup...", tweet_counter
        url = APIUrl
        try:
                    
                    params = {"oauth_version":"1.0","oauth_nonce": oauth2.generate_nonce(),"oauth_timestamp":int(time.time())}
                    params["oauth_consumer_key"] = (pool_oauth[pool_ind][0]).key
                    params["oauth_token"] = (pool_oauth[pool_ind][1]).key
                    params["user_id"] =user_id #list of 100 tweets
                    #params["include_entities"] ="" #optional
                    #params["trim_user"] =""#optional
                    #params["map"] = ""#optional
                 
                    req = oauth2.Request(method="GET", url=url, parameters=params)
                    signature_method = oauth2.SignatureMethod_HMAC_SHA1()# HMAC, twitter use sha-1 160 bit encryption
                    req.sign_request(signature_method, (pool_oauth[pool_ind][0]), (pool_oauth[pool_ind][1]))
                    headers = req.to_header()
                    url = req.to_url()
                
                 
                    response = urllib2.Request(url)
                    data = json.load(urllib2.urlopen(response))#format results as json
                    if len(data[0])>0:
                        saveToDB(data[0])
                        active_users.append(user_id)

                    else:
                        print "this user is unreachable", user_id
                        unreachable_user.append(user_id)
                        unreachable.write(str(user_id)+'\n')
                        print data 
                        print "size of unreachable_user: ", len(unreachable_user)
                    #print input_ids
                    #print output_ids
                    #print "input size",len(input_ids)
                    #print "output size",len(output_ids)
                    #print deleted_ids
                    user_id=""
                    
                    #print "global counter:", globalCounter
        except Exception, ex:
                print 'Exception: '+str(ex)
                print "this user is unreachable", user_id
                unreachable_user.append(user_id)
                unreachable.write(str(user_id)+'\n')
                    
        finally:
                time.sleep(4)
                pool_ind+=1
                pool_ind= pool_ind % 4
                if tweet_counter %500==0:
                    print tweet_counter
    pickle.dump(unreachable_user, open( "unreachable_user_after_calling_API_lookup.p", "wb" ) )
    return active_users
                    
def main():
    
    globalCounter=0
    global size_active
    import pickle
    users=pickle.load( open( "list_of_usersIDS_with_unreachable_tweets_coup2016.p", "rb" ) )

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
    APIUrl='https://api.twitter.com/1.1/users/lookup.json'
    '''
    here goes some mongodb code
    '''
    #************thisis the database we are reading from************8
    
    #collection=db['combined_TurkishE_KnownUnk_NoWA']
    


    a_users=get_user_api(users,APIUrl,pool_oauth,globalCounter)
    print len(a_users)
    pickle.dump(a_users, open( "Activeusers_with_deleted_tweets.p", "wb" ) )
    
    
    
    
if __name__=="__main__":
    main()