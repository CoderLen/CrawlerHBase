package org.crawler.util;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Calendar;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpDownloader implements Callable<String> {
	URLConnection connection;
	FileChannel outputChann;
	public static volatile int count = 0;

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {

		ExecutorService poll = Executors.newFixedThreadPool(100);

		for (int i = 0; i < 100; i++) {
			Calendar now = Calendar.getInstance();
			String fileName = "D:/hadoop-2.4.0/" + now.get(Calendar.YEAR) + "年"
					+ (now.get(Calendar.MONTH) + 1) + "月"
					+ now.get(Calendar.DAY_OF_MONTH) + "日--" + i + ".txt";
			poll.submit(new HttpDownloader("http://www.sina.com",
					(new FileOutputStream(fileName)).getChannel()));
		}

		poll.shutdown();

		long start = System.currentTimeMillis();
		while (!poll.isTerminated()) {
			Thread.sleep(1000);
			System.out.println("已运行"
					+ ((System.currentTimeMillis() - start) / 1000) + "秒，"
					+ HttpDownloader.count + "个任务还在运行");
		}
	}

	public HttpDownloader(String url, FileChannel fileChannel) throws Exception {
		synchronized (HttpDownloader.class) {
			count++;
		}
		connection = (new URL(url)).openConnection();
		this.outputChann = fileChannel;
	}

	public String call() throws Exception {
		connection.connect();
		InputStream inputStream = connection.getInputStream();
		ReadableByteChannel rChannel = Channels.newChannel(inputStream);
		outputChann.transferFrom(rChannel, 0, Integer.MAX_VALUE);
		// System.out.println(Thread.currentThread().getName() + " completed!");
		inputStream.close();
		outputChann.close();
		synchronized (HttpDownloader.class) {
			count--;
		}
		return null;
	}
}