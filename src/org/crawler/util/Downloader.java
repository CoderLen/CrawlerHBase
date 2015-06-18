package org.crawler.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.io.Text;

public class Downloader{
	public static String Download(Text pageURL){
		  StringBuilder pageHTML = new StringBuilder(); 
		   String utlStr =  pageURL.toString();
	        try { 
	            URL url = new URL(utlStr); 
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection(); 
	            connection.setRequestProperty("User-Agent", "MSIE 7.0"); 
	            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),"gb2312")); 
	            String line = null; 
	            while ((line = br.readLine()) != null) { 
	                pageHTML.append(line); 
	               // pageHTML.append("\r\n"); 
	            } 
	            
	            connection.disconnect(); 
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        } 
//	        System.out.println(pageHTML.toString());
	        return pageHTML.toString();
	}
	
	public static void main(String[] agrs){
		Text pageURL = new Text();
		pageURL.set("http://news.sohu.com/20150212/n408970549.shtml");
		Downloader.Download(pageURL);
	}
}