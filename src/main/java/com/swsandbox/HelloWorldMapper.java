package com.swsandbox;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:24 PM
 */
public class HelloWorldMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, Text>
{
    @Override
    protected void map(Map<String, ByteBuffer> key, Map<String, ByteBuffer> value, Context context) throws IOException, InterruptedException
    {
        //iterate over the map of values
        for (Map.Entry<String, ByteBuffer> column : value.entrySet())
        {
            if (!"empty".equals(column.getKey().toLowerCase()))
            {
                System.out.println("key: (" + convertByteBufferToString(key) + ") -> " + column.getKey() + " = " + ByteBufferUtil.toInt(column.getValue()) + ", done");
            }
            else
            {
                System.out.println("key: (" + convertByteBufferToString(key) + ") -> " + column.getKey() + " = empty..., done");
            }
        }
        context.write(new Text(convertByteBufferToString(key)), new Text(" says hello!"));
    }

    private String convertByteBufferToString(Map<String, ByteBuffer> keys)
    {
        String result = "";
        try
        {
            int c = 0;
            for (ByteBuffer key : keys.values())
            {
                c++;
                if (keys.size() == 1)
                {
                    result = result + ByteBufferUtil.string(key);
                }
                else
                {
                    if (c == keys.size())
                    {
                        result = result + ByteBufferUtil.string(key);
                    }
                    else
                    {
                        result = result + ByteBufferUtil.string(key) + ":";
                    }
                }
            }
        }
        catch (CharacterCodingException e)
        {
            e.printStackTrace();
        }
        return result;
    }
}
