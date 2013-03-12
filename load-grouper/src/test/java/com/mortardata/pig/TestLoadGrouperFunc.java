package com.mortardata.pig;

import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


 public class TestLoadGrouperFunc {
    static PigServer pig;

    private static final String dataDir = "build/test/tmpdata/";
    private static final String input1 = "load_grouper_input1";

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);

            Util.createLocalInputFile(dataDir + input1,
                    new String[] {
                            "123\tabc",
                            "456\tdef"
                    });
        }
        catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void loadGrouper() throws IOException {
        pig.registerQuery(
                "data = load '" + dataDir + input1 + "' " +
                        "using com.mortardata.pig.LoadGrouperFunc();"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
                "(" + input1 + ",0,2)",
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
}