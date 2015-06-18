package org.crawler.main;

import org.apache.hadoop.util.ToolRunner;
import org.crawler.crawlerdriver.CrawlerDriver;
import org.crawler.htmltoxmldriver.HtmlToXMLDriver;
import org.crawler.optimizerdriver.OptimizerDriver;
import org.crawler.parserdriver.ParserDriver;

public class Crawler {
	
	private CrawlerDriver crawler;
	private ParserDriver parser;
	private OptimizerDriver optimizer;
	private HtmlToXMLDriver xml_convert;
	
	public Crawler() {
		// TODO Auto-generated constructor stub
		crawler = new CrawlerDriver();
		parser = new ParserDriver();
		optimizer = new OptimizerDriver();
		xml_convert = new HtmlToXMLDriver();
	}
	
	public static void main(String[] args) throws Exception{
		Crawler crawleMain = new Crawler();
		
		if(true){
			
			int depth = 1;
			
			for(int i=1;i<=depth;i++){
				System.out.println("�� " + i + " ��:��ʼִ��CrawlerDriver,����ҳ��");
				int crawlerCode = ToolRunner.run(crawleMain.crawler,args);
				if(crawlerCode == 1){
					System.out.println("�� " + i + " ��:��ʼִ��ParserDriver,����ҳ����ȡURL");
					int parserCode = ToolRunner.run(crawleMain.parser,args);
					if(parserCode == 1){
						System.out.println("�� " + i + " ��:��ʼִ��OptimizerDriver,�Ż�URL");
						int optimizerCode = ToolRunner.run(crawleMain.optimizer, args);
						if(optimizerCode == 1){
							System.out.println("�� " + i + " ��:��ʼִ��HtmlToXMLDriver,��ҳ��ת����XML����");
							int convertCode = ToolRunner.run(crawleMain.xml_convert,args);
							if(convertCode == 1){
								System.out.println("�� " + i + " �㣺ץȡ��ϣ�");
							}
						}
					}
				}
			}
			
		}
	}
	
	
}
