package com.kafka.streams.utils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Inflater;

/**
 * Use to read content of files.
 * Mainly used for reading sql queries from .sql files
 **/
public class FileUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    private Map<String, String> cache = new HashMap<>();
    public static final String TEST_INT_CONVERSION = "test int conversion";
    public static final String TEST_DOUBLE_CONVERSION = "test Double conversion";
    public static final String TEST_STRING_CONVERSION = "test String conversion";
    public static final String ENTERED_FILE_UTIL_CONTENT_OF = "entered FileUtil.contentOf(\"{}\")";
    public static final String ENTERED_FILE_UTIL_READ_FILE = "entered FileUtil.readFile(\"{}\")";

    public String contentOf(String path) throws IOException {
        LOG.info(ENTERED_FILE_UTIL_CONTENT_OF, path);
        String contents = this.cache.get(path);
        if (null == contents) {
            synchronized (this.cache) {
                contents = this.cache.get(path);
                if (null == contents) {
                    contents = FileUtil.readFile(path);
                    if (contents != null) {
                        this.cache.put(path, contents);
                    }
                }
            }
        }
        return contents;
    }

    public String contentOf(String path, String exceptionMessage) throws Exception {
        try {
            return contentOf(path);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception();
        }
    }

    public static String readFile(String path) throws IOException {
        LOG.info(ENTERED_FILE_UTIL_READ_FILE, path);
        InputStream is = FileUtil.class.getResource(path).openStream();
        String ret = IOUtils.toString(is);
        is.close();
        return ret;
    }

    public static byte[] unZip(InputStream in) {
        try {
            byte[] inBytes = IOUtils.toByteArray(in);
            if (inBytes != null) {
                Inflater inflater = new Inflater();
                inflater.setInput(inBytes);
                byte[] buffer = new byte[4096];
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                while (!inflater.finished()) {
                    int inflaterSize = inflater.inflate(buffer);
                    byteArrayOutputStream.write(buffer, 0, inflaterSize);
                }
                byteArrayOutputStream.close();
                return byteArrayOutputStream.toByteArray();
            }
        } catch (Exception e) {
            LOG.error("Exception :: unZip() :: ", e);
        }
        return new byte[0];
    }
}
