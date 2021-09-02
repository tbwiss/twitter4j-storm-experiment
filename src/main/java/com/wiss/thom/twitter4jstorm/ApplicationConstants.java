/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.wiss.thom.twitter4jstorm;

/**
 * Application Specific Constants. 
 */
public class ApplicationConstants {
 
    // Twitter application specific secret constants.   
    public static final String CONSUMER_KEY_KEY = "xxxx";
    public static final String CONSUMER_SECRET_KEY = "xxx";
    public static final String ACCESS_TOKEN_KEY = "xx-xx";
    public static final String ACCESS_TOKEN_SECRET_KEY = "xxx";
 
    // constants
    public static final String NOT_AVAILABLE = "Not Available";
    public static final String EMPTY = "";
 
    // Topology Constants 
    public static final String TOPOLOGY_NAME = "twitter-topology";
    public static final String TWITTER_SPOUT_ID = "twitterSpout";
    public static final String DETAILS_BOLT_ID = "detailsExtractorBolt";
    public static final String RETWEET_DETAILS_BOLT_ID = "retweetDetailsExtractorBolt";
    public static final String FILE_WRITER_BOLT_ID = "fileWriterBolt";
    public static final String ORIENTDB_WRITER_BOLT_ID = "OrientDBWriterBolt";
}
