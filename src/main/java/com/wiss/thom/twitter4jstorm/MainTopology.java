/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.wiss.thom.twitter4jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class MainTopology {
 
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // TopologyBuilder instance. Defines the data flow between the components in the topology.
        TopologyBuilder topologyBuilder = new TopologyBuilder();    
        // Register the twitter spout and assign it a unique id.
        topologyBuilder.setSpout(ApplicationConstants.TWITTER_SPOUT_ID, new TwitterSpout());    
        /*
         *  TwitterSpout ----> DetailsExtractorBolt
         *  Assigned parallelism 2 for DetailsExtractorBolt.
         */
        topologyBuilder.setBolt(ApplicationConstants.DETAILS_BOLT_ID, new DetailsExtractorBolt(), 2)
        .shuffleGrouping(ApplicationConstants.TWITTER_SPOUT_ID);    
        /*
         *  TwitterSpout ----> RetweetDetailsExtractorBolt
         *  Assigned parallelism 2 for RetweetDetailsExtractorBolt.
         */        
        topologyBuilder.setBolt(ApplicationConstants.RETWEET_DETAILS_BOLT_ID, new RetweetDetailsExtractorBolt(), 2)
        .shuffleGrouping(ApplicationConstants.TWITTER_SPOUT_ID);    
        
        // Join DetailsExtractorBolt and RetweetDetailsExtractorBolt ----> FileWriterBolt
        topologyBuilder.setBolt(ApplicationConstants.FILE_WRITER_BOLT_ID, new FileWriterBolt())
        .fieldsGrouping(ApplicationConstants.DETAILS_BOLT_ID, new Fields("title"))
        .fieldsGrouping(ApplicationConstants.RETWEET_DETAILS_BOLT_ID, new Fields("title"));    
                
        
        // Join DetailsExtractorBolt and RetweetDetailsExtractorBolt ----> OrientDBWriterBolt
        topologyBuilder.setBolt(ApplicationConstants.ORIENTDB_WRITER_BOLT_ID, new OrientDBWriterBolt())
        .fieldsGrouping(ApplicationConstants.DETAILS_BOLT_ID, new Fields("title"))
        .fieldsGrouping(ApplicationConstants.RETWEET_DETAILS_BOLT_ID, new Fields("title"));   
                
        
        
        // Config instance. It defines topology's runtime behavior.
        Config config = new Config();    
        // To run storm in local mode.
 
        if (args != null && args.length > 0) {
              config.setNumWorkers(1);
              StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            }
            else {
                final LocalCluster cluster = new LocalCluster();
                // deploy topology in local mode.
                cluster.submitTopology(ApplicationConstants.TOPOLOGY_NAME, config, topologyBuilder.createTopology());
                // This method will kill the topology while shutdown the JVM.
                Runtime.getRuntime().addShutdownHook(new Thread()    {
                    @Override
                    public void run()    {
                    cluster.killTopology(ApplicationConstants.TOPOLOGY_NAME);
                    cluster.shutdown();
                }
                });
            }
    }
 
}