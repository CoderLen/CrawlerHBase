package org.crawler.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser {

	private static Logger logger = LoggerFactory.getLogger(Parser.class);
	private static int count = 1;
	
	private static Configuration conf =  new Configuration();
	
	static{
		conf.addResource("app-config.xml");
	}

	public static Set<String> parser(String html){
		System.out.println("��ʼ������ҳ.....");
		Set<String> urls = new HashSet<String>();
		String regex = "<a.*?/a>";
		Pattern pt = Pattern.compile(regex);
		Matcher matcher = pt.matcher(html);
		while(matcher.find()){
            String webfilter =  conf.get("url.filter.regex");
			Matcher myurl = Pattern.compile(webfilter).matcher(matcher.group());
			while(myurl.find()){
				String  url = myurl.group();
						urls.add(url);
						logger.info("��ȡ��ַ"+count+":"+ url);
						count++;
			}
		}
		logger.info("������ҳ��ɣ�");
		return urls;
	}
	
	public static void main(String[] args){
	      String webfilter =  conf.get("url.filter.regex");
          System.out.print(webfilter);
	}
}
