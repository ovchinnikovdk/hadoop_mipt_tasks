package ru.mipt.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.entity.VisitedDomain;
import ru.mipt.writable_comparable.SocnetDomain;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by dmitry on 12.03.17.
 */

/*
* Class UrlCountMapper
* Takes text value and parses url
*
* */
public class UrlCountMapper extends Mapper<LongWritable, Text, SocnetDomain, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String domain = getDomainName(fields[0]);
        String[] socnets = fields[1].split(";");
        for (String socnet : socnets) {
            String[] counts = socnet.split(":");
            context.write(new SocnetDomain(counts[0], domain), new IntWritable(Integer.valueOf(counts[1])));
        }
    }


    private String getDomainName(String url) throws MalformedURLException {
        if (!url.startsWith("http") && !url.startsWith("https")) {
            url = "http://" + url;
        }
        URL netUrl = new URL(url);
        String host = netUrl.getHost();
        if(host.startsWith("www")){
            host = host.substring(4);
        }
        return host;
    }
}
