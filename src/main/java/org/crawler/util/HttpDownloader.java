package org.crawler.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class HttpDownloader implements Callable<String> {
    private URLConnection connection;
    private FileChannel outputChann;
    private AtomicInteger count;

    public HttpDownloader(String url, FileChannel fileChannel, AtomicInteger count) throws Exception {
        this.count = count;
        connection = (new URL(url)).openConnection();
        this.outputChann = fileChannel;
    }

    public static void main(String[] args) throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(100));
        LocalDate localeDate = LocalDate.now();
        AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            String fileName = "files" + File.separator + localeDate.toString() + "_" + i + ".txt";
            executor.submit(new HttpDownloader("https://www.sina.com",
                    (new FileOutputStream(fileName)).getChannel(), count));
        }
        long start = System.currentTimeMillis();
        while (!executor.isTerminated()) {
            Thread.sleep(1000);
            log.info("已运行"
                    + ((System.currentTimeMillis() - start) / 1000) + "秒，"
                    + count.get() + "个任务还在运行");
        }
        executor.shutdown();
    }

    @Override
    public String call() throws Exception {
        connection.connect();
        this.count.incrementAndGet();
        InputStream inputStream = connection.getInputStream();
        ReadableByteChannel rChannel = Channels.newChannel(inputStream);
        outputChann.transferFrom(rChannel, 0, Integer.MAX_VALUE);
        inputStream.close();
        outputChann.close();
        Thread.sleep(2000);
        synchronized (HttpDownloader.class) {
            count.decrementAndGet();
        }
        return null;
    }
}