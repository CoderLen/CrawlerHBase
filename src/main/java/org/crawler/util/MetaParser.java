package org.crawler.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaParser {

    public static boolean CheckMetaIndex(String html) {
        String regex = "<meta.*?/>";
        Pattern pt = Pattern.compile(regex);
        Matcher matcher = pt.matcher(html);
        while (matcher.find()) {
            if (matcher.group().contains("content=\"index\"")) {
                return true;
            }
        }
        return false;
    }

    public static boolean CheckMetaFollow(String html) {
        String regex = "<meta.*?/>";
        Pattern pt = Pattern.compile(regex);
        Matcher matcher = pt.matcher(html);
        while (matcher.find()) {
            if (matcher.group().contains("content=\"follow\"")) {
                return true;
            }
        }
        return false;
    }
}
