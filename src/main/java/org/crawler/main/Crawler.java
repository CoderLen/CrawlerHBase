package org.crawler.main;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.ToolRunner;
import org.crawler.crawlerdriver.CrawlerDriver;
import org.crawler.htmltoxmldriver.HtmlToXMLDriver;
import org.crawler.optimizerdriver.OptimizerDriver;
import org.crawler.parserdriver.ParserDriver;

@Slf4j
public class Crawler {

    private CrawlerDriver crawler;
    private ParserDriver parser;
    private OptimizerDriver optimizer;
    private HtmlToXMLDriver xml_convert;

    public Crawler() {
        crawler = new CrawlerDriver();
        parser = new ParserDriver();
        optimizer = new OptimizerDriver();
        xml_convert = new HtmlToXMLDriver();
    }

    public static void main(String[] args) throws Exception {
        Crawler crawleMain = new Crawler();

        if (true) {
            int depth = 1;
            for (int i = 1; i <= depth; i++) {
                log.info("? " + i + " ?:????CrawlerDriver,????");
                int crawlerCode = ToolRunner.run(crawleMain.crawler, args);
                if (crawlerCode == 1) {
                    log.info("? " + i + " ?:????ParserDriver,??????URL");
                    int parserCode = ToolRunner.run(crawleMain.parser, args);
                    if (parserCode == 1) {
                        log.info("? " + i + " ?:????OptimizerDriver,??URL");
                        int optimizerCode = ToolRunner.run(crawleMain.optimizer, args);
                        if (optimizerCode == 1) {
                            log.info("? " + i + " ?:????HtmlToXMLDriver,??????XML??");
                            int convertCode = ToolRunner.run(crawleMain.xml_convert, args);
                            if (convertCode == 1) {
                                log.info("? " + i + " ???????");
                            }
                        }
                    }
                }
            }

        }
    }

}
