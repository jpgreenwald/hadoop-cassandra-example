package com.swsandbox;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:25 PM
 */
public class HelloWorldReducer extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        System.out.println("text: " + key + ": ");
        for (Text t : values)
        {
            Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
            keys.put("name", ByteBufferUtil.bytes(key.toString()));

            List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
            variables.add(ByteBufferUtil.bytes(t.toString()));

            context.write(keys, variables);
        }
    }
}
