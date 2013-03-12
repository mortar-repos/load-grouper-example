package com.mortardata.pig;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LoadGrouperFunc extends PigStorage {
    Log log = LogFactory.getLog(LoadGrouperFunc.class);
    PigSplit split;
    boolean isDone;
    
    //Parameters for specific grouping implementation
    int count = 0;

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        super.prepareToRead(reader, split);
        this.split = split;
        isDone = false;
    }
    
    @Override
    public Tuple getNext() throws IOException {
        if (isDone) {
            log.info("Already done.");
            return null;
        }

        Tuple row = super.getNext();
        while (row != null) {
            processRow(row);
            row = super.getNext();
        }

        isDone = true;
        return getResult();
    }

    /**
     * Called for each individual row.  Let's you build up the final result interatively
     * without maintaining all rows in memory.
     * 
     * @param row
     */
    private void processRow(Tuple row) {
        count += 1;
    }

    /**
     * Called at the end after we've seen every row in the split.  If you need to perform 
     * a calculation on the whole result set (Ex. calculating a median) use processRow to 
     * build up a collection of tuples and operate them in this method.
     *  
     * @return
     * @throws ExecException
     */
    private Tuple getResult() throws ExecException {
        Tuple result = TupleFactory.getInstance().newTuple(3);

        Path filePath = ((FileSplit) split.getWrappedSplit()).getPath();
        result.set(0, filePath.getName());
        result.set(1, split.getSplitIndex());
        result.set(2, count);
        return result;
    }
}
