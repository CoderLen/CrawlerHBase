package org.crawler.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class Parser {

    private static Logger logger = LoggerFactory.getLogger(Parser.class);
    private static int count = 1;

    private static Configuration conf = new Configuration();

    static {
        conf.addResource("app-config.xml");
    }

    public static Set<String> parser(String html) {
        log.info("开始分析网页.....");
        Set<String> urls = new HashSet<String>();
        String regex = "<a.*?/a>";
        Pattern pt = Pattern.compile(regex);
        Matcher matcher = pt.matcher(html);
        while (matcher.find()) {
            String webfilter = conf.get("url.filter.regex");
            Matcher myurl = Pattern.compile(webfilter).matcher(matcher.group());
            while (myurl.find()) {
                String url = myurl.group();
                urls.add(url);
                logger.info("获取网址" + count + ":" + url);
                count++;
            }
        }
        logger.info("分析网页完成！");
        return urls;
    }

    public static void main(String[] args) {
        String webfilter = conf.get("url.filter.regex");
        System.out.print(webfilter);
    }
}
