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
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author Thomas Wiss
 */
public class OrientDBWriterBolt implements IRichBolt {

    private OrientGraphFactory factory;

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        factory = new OrientGraphFactory("plocal:C:\\OrientDB\\orientdb-community-2.1.8\\databases\\twitter", "root", "root")
                .setupPool(1, 10);
    }

    @Override
    public void execute(Tuple tuple) {
        String screenName = "";
        String userName = "";
        Long statusID = null;
        String url = "";
        String actualURL = "";
        Date publishedDate = null;
        boolean isRetweet = false;
        String retweetScreenName = "";
        String retweetUserName = "";
        String title = "";
        OrientGraph graph = factory.getTx();
        try {
            //checks whether coming tuple is from DetailsExtractorBolt or RetweetDetailsExtractorBolt.
            if ("retweetDetailsExtractorBolt".equals(tuple.getSourceComponent())) {

                title = (String) tuple.getValueByField("title");
                isRetweet = (Boolean) tuple.getValueByField("isRetweet");
                retweetScreenName = (String) tuple.getValueByField("retweetScreenName");
                retweetUserName = (String) tuple.getValueByField("retweetUserName");
                FileDetails fileDetails = TweetDetailsManager.get(title);
                if (fileDetails == null) {
                    fileDetails = new FileDetails();
                }
                fileDetails.setIsRetweet(String.valueOf(isRetweet));
                fileDetails.setRetweetScreenName(retweetScreenName);
                fileDetails.setRetweetUserName(retweetUserName);
                fileDetails.isRetweetBoltExec(true);
                TweetDetailsManager.addDetails(title, fileDetails);
                if (isRetweet) {
                    System.out.println(".... RETWEET ::::::::   userName: " + userName + "   retweetedUserName: " + retweetUserName);
                    try {
                        Vertex vertexUser = null;
                        /*
                        try {
                            vertexUser = graph.getVertex(userName);
                        } catch (Exception e) {
                            System.err.println("tempVertex get Vertex: " + e);
                        }
                                
                     
                            vertexUser = graph.addVertex(userName);
                            vertexUser.setProperty("title", title);
                            vertexUser.setProperty("userName", userName);
                            vertexUser.setProperty("screenName", screenName);
                        */
                        Vertex vertexRetweetUser = graph.addVertex(retweetUserName);
                        vertexRetweetUser.setProperty("title", title);
                        vertexRetweetUser.setProperty("retweetUserName", retweetUserName);
                        vertexRetweetUser.setProperty("retweetScreenName", retweetScreenName);
                        Edge retweeted = graph.addEdge(null, vertexRetweetUser, vertexUser, "retweeted");
                        graph.commit();
                    } catch (Exception e) {
                        System.err.println("... ReTWeet: Exception: " + e);
                        graph.rollback();
                    }
                }
            } else if ("detailsExtractorBolt".equals(tuple.getSourceComponent())) {
                screenName = (String) tuple.getValueByField("screenName");
                userName = (String) tuple.getValueByField("userName");
                statusID = (Long) tuple.getValueByField("statusID");
                url = (String) tuple.getValueByField("url");
                actualURL = (String) tuple.getValueByField("actualURl");
                title = (String) tuple.getValueByField("title");
                publishedDate = (Date) tuple.getValueByField("publishedDate");
                FileDetails fileDetails = TweetDetailsManager.get(title);
                if (fileDetails == null) {
                    fileDetails = new FileDetails();
                }
                fileDetails.setScreenName(screenName);
                fileDetails.setUserName(userName);
                fileDetails.setStatusID(String.valueOf(statusID));
                fileDetails.setUrl(url);
                fileDetails.setActualURL(actualURL);
                fileDetails.setTitle(title);
                fileDetails.setPublishedDate(String.valueOf(publishedDate));
                fileDetails.isDetailsBoltExec(true);
                TweetDetailsManager.addDetails(title, fileDetails);
                try {
                    Vertex vertexUser = graph.addVertex(userName);
                    vertexUser.setProperty("title", title);
                    vertexUser.setProperty("userName", userName);
                    vertexUser.setProperty("screenName", screenName);
                    graph.commit();
                } catch (Exception e) {
                    graph.rollback();
                }
            }
        } catch (Exception e) {
            System.err.println("geTX::  " + e);
        }
    }

    @Override
    public void cleanup() {
        //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        //To change body of generated methods, choose Tools | Templates.
        return null;
    }

}
