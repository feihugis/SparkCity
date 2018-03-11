/**
 * FILE: ShapeInputFormat.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeInputFormat.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.shapefile.meta.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.CombineShapeReader;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.config.ConfigParameter;
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import edu.gmu.stc.vector.shapefile.meta.index.reader.CombineShapeFileMetaIndexReader;

public class ShapeFileMetaIndexInputFormat extends CombineFileInputFormat<ShapeKey, ShapeFileMeta> {
    public RecordReader<ShapeKey, ShapeFileMeta> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineShapeFileMetaIndexReader();
    }

    /**
     * enforce isSplitable() to return false so that every getSplits() only return one InputSplit
     * @param context
     * @param file
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // get input paths and assign a split for every single path
        String path = job.getConfiguration().get(ConfigParameter.INPUT_DIR_PATH);
        String[] paths = path.split(",");
        List<InputSplit> splits = new ArrayList<>();
        for(int i = 0; i < paths.length; ++i){
            job.getConfiguration().set(ConfigParameter.INPUT_DIR_PATH, paths[i]);
            splits.add(super.getSplits(job).get(0));
        }
        return splits;
    }
}

