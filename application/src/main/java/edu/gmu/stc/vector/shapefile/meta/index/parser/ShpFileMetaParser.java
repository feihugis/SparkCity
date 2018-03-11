/**
 * FILE: ShpFileParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShpFileParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.shapefile.meta.index.parser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.commons.io.EndianUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.gmu.stc.vector.shapefile.meta.ShpMeta;

public class ShpFileMetaParser implements Serializable, ShapeFileConst{

  public static GeometryFactory geometryFactory = new GeometryFactory();

    /** lenth of file in bytes */
    private long fileLength = 0;

    /** remain length of bytes to parse */
    private long remainLength = 0;

    /** input reader */
    private final SafeReader reader;

    /**
     * A limited wrapper around FSDataInputStream providing proper implementations
     * for methods of FSDataInputStream which are known to be broken on some platforms.
     */
    private static final class SafeReader
    {
        private final FSDataInputStream input;

        private SafeReader(FSDataInputStream input) {
            this.input = input;
        }

        public int readInt() throws IOException {
            byte[] bytes = new byte[ShapeFileConst.INT_LENGTH];
            input.readFully(bytes);
            return ByteBuffer.wrap(bytes).getInt();
        }

        public double readDouble() throws IOException {
          byte[] bytes = new byte[ShapeFileConst.DOUBLE_LENGTH];
          input.readFully(bytes);
          return ByteBuffer.wrap(bytes).getDouble();
        }

        public void skip(int numBytes) throws IOException {
            input.skip(numBytes);
        }

        public void read(byte[] buffer, int offset, int length) throws IOException {
            input.readFully(buffer, offset, length);
        }

        public long getPos() throws IOException {
            return this.input.getPos();
        }
    }

    /**
     * create a new shape file parser with a input source that is instance of DataInputStream
     * @param inputStream
     */
    public ShpFileMetaParser(FSDataInputStream inputStream) {
        reader = new SafeReader(inputStream);
    }

    /**
     * extract and validate information from .shp file header
     * @throws IOException
     */
    public void parseShapeFileHead()
            throws IOException
    {
        reader.skip(INT_LENGTH);
        reader.skip(HEAD_EMPTY_NUM * INT_LENGTH);
        fileLength = 2 * ((long)reader.readInt() - HEAD_FILE_LENGTH_16BIT);
        remainLength = fileLength;
        // Skip 2 integers: file version and token type
        reader.skip(2 * INT_LENGTH);
        // if bound box is not referenced, skip it
        reader.skip(HEAD_BOX_NUM * DOUBLE_LENGTH);
    }

    /**
     * abstract information from record header and then copy primitive bytes data of record to a primitive record.
     * @return
     * @throws IOException
     */
    public ShpMeta parseRecordPrimitiveContent() throws IOException{
        // get length of record content
        int contentLength = reader.readInt();
        long recordLength = 2 * (contentLength + 4);
        remainLength -= recordLength;
        int typeID = EndianUtils.swapInteger(reader.readInt());
        int length = contentLength * 2 - INT_LENGTH; // exclude the 4 bytes we read for shape type
        long offset = reader.getPos();
        double[] bounds = new double[4]; //in the order Xmin, Ymin, Xmax, Ymax

        for (int i = 0; i < bounds.length; i++) {
          bounds[i] = reader.readDouble();
        }

        reader.skip(length - 4 * DOUBLE_LENGTH);

        BoundBox boundBox = new BoundBox(bounds);
        return new ShpMeta(typeID, offset, length, boundBox);
    }

    /**
     * abstract information from record header and then copy primitive bytes data of record to a primitive record.
     * @return
     * @throws IOException
     */
    public ShpMeta parseRecordPrimitiveContent(int length) throws IOException{
        // get length of record content
        int contentLength = reader.readInt();
        long recordLength = 2 * (contentLength + 4);
        remainLength -= recordLength;
        int typeID = EndianUtils.swapInteger(reader.readInt());

        long offset = reader.getPos();
        double[] bounds = new double[4]; //in the order Xmin, Ymin, Xmax, Ymax

        for (int i = 0; i < bounds.length; i++) {
          bounds[i] = EndianUtils.swapDouble(reader.readDouble());
        }

        reader.skip(length - 4 * DOUBLE_LENGTH);

        BoundBox boundBox = new BoundBox(bounds);
        return new ShpMeta(typeID, offset, length, boundBox);
    }

    /**
     * abstract id number from record header
     * @return
     * @throws IOException
     */
    public int parseRecordHeadID() throws IOException{
        return reader.readInt();
    }

    /**
     * get current progress of parsing records.
     * @return
     */
    public float getProgress(){
        return 1 - (float)remainLength / (float) fileLength;
    }

}
