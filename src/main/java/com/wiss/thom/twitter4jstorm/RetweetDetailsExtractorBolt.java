/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.wiss.thom.twitter4jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import twitter4j.Status;
import backtype.storm.tuple.Fields;

/**
 *
 * @author Thomas Wiss
 */
public class RetweetDetailsExtractorBolt implements IRichBolt{
    
    private OutputCollector collector;
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
 
    }
 
    /* (non-Javadoc)
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     * This emits the retweet details.
     */
    @Override
    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValueByField("status");
        String title = status.getText();
        boolean isRetweet = status.isRetweet();
        String retweetScreenName = ApplicationConstants.EMPTY;
        String retweetUserName = ApplicationConstants.EMPTY;
        // if tweet is retweeted get the details else assign default value.
        if(isRetweet) {
            Status retweet = status.getRetweetedStatus();
            retweetScreenName = retweet.getUser().getScreenName();
            retweetUserName = retweet.getUser().getName();
        } else {
            retweetScreenName = ApplicationConstants.NOT_AVAILABLE;
            retweetUserName = ApplicationConstants.NOT_AVAILABLE;
        }
        collector.emit(new Values(title,isRetweet,retweetScreenName, retweetUserName));    
    }
 
    /* (non-Javadoc)
     * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     * 
     * This saves the OutputCollector as an instance variable to be used to emit tuples.
     */
    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;        
    }
    // Declare output fields.
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("title", "isRetweet", "retweetScreenName", "retweetUserName"));
    }
 
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
