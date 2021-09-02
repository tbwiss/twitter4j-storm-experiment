/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.wiss.thom.twitter4jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
 
import twitter4j.DirectMessage;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
 
/**
 * This class emits stream of tweets from the twitter account.
 *
 */
public class TwitterSpout implements IRichSpout {
 
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;    
 
    /**
     * The initialization method. 
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        queue = new LinkedBlockingQueue<Status>(100);
        /** 
         * The UserStreamListener is a twitter4j API, which can be added to a Twitter stream, 
         * and will execute methods every time a message comes in through the stream.
         */
        UserStreamListener  listener = new UserStreamListener() {
            @Override
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub
 
            }
            // This method executes every time a new tweet comes in.
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }
 
            @Override
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserProfileUpdate(User arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListUpdate(User arg0, UserList arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListUnsubscription(User arg0, User arg1, UserList arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListSubscription(User arg0, User arg1, UserList arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListMemberDeletion(User arg0, User arg1, UserList arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListMemberAddition(User arg0, User arg1, UserList arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListDeletion(User arg0, UserList arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUserListCreation(User arg0, UserList arg1) {
                // TODO Auto-generated method stub
 
            }
 
     
 
            @Override
            public void onUnfavorite(User arg0, User arg1, Status arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onUnblock(User arg0, User arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onFriendList(long[] arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onFollow(User arg0, User arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onFavorite(User arg0, User arg1, Status arg2) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onDirectMessage(DirectMessage arg0) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onDeletionNotice(long arg0, long arg1) {
                // TODO Auto-generated method stub
 
            }
 
            @Override
            public void onBlock(User arg0, User arg1) {
                // TODO Auto-generated method stub
 
            }
        };
        // ConfigurationBuilder object.
        ConfigurationBuilder cb = new ConfigurationBuilder();        
        cb.setOAuthConsumerKey(ApplicationConstants.CONSUMER_KEY_KEY);
        cb.setOAuthConsumerSecret(ApplicationConstants.CONSUMER_SECRET_KEY);
        cb.setOAuthAccessToken(ApplicationConstants.ACCESS_TOKEN_KEY);
        cb.setOAuthAccessTokenSecret(ApplicationConstants.ACCESS_TOKEN_SECRET_KEY);
        
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = {"Football","FIFA"};

        fq.track(keywords);
 
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
 
    }
 
    @Override
    public void close() {
        twitterStream.shutdown();
    }
 
    @Override
    public void activate() {
 
    }
 
    @Override
    public void deactivate() {
 
    }
 
    // Spout emits tuples to the output collector.
    @Override
    public void nextTuple() {
        // Status object from the queue.
        Status status = queue.poll();     
        if(status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }
 
    @Override
    public void ack(Object msgId) {
 
    }
 
    @Override
    public void fail(Object msgId) {
 
    }
    // Emits single stream of tuple containing a single field status.
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }
 
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
 
}