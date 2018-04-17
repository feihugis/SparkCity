package edu.gmu.stc.vector.shapefile.reader;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.factory.ReferencingObjectFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import edu.gmu.stc.vector.sourcedata.Attribute;

/**
 * Created by Fei Hu on 1/26/18.
 */
public class GeometryReaderUtil {
  /** suffix of attribute file */
  public final static String DBF_SUFFIX = ".dbf";

  /** suffix of shape record file */
  public final static String SHP_SUFFIX = ".shp";

  /** suffix of index file */
  public final static String SHX_SUFFIX = ".shx";


  public static Geometry readGeometryWithAttributes(FSDataInputStream shpDataInputStream,
                                                    FSDataInputStream dbfDataInputStream,
                                                    ShapeFileMeta shapeFileMeta,
                                                    DbfParseUtil dbfParseUtil,
                                                    GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(),
                            content,
                            0,
                            shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());


    PrimitiveShape value = new PrimitiveShape(shpRecord);

    byte[] primitiveBytes = new byte[shapeFileMeta.getDbf_length()];
    dbfDataInputStream.read(shapeFileMeta.getDbf_offset(),
                            primitiveBytes,
                            0,
                            shapeFileMeta.getDbf_length());
    String attributes = dbfParseUtil.primitiveToAttributes(ByteBuffer.wrap(primitiveBytes));
    value.setAttributes(attributes);

    return value.getShape(geometryFactory);
  }

  public static Geometry readGeometry(FSDataInputStream shpDataInputStream,
                                      ShapeFileMeta shapeFileMeta,
                                      GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(), content,
                            0, shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());
    PrimitiveShape value = new PrimitiveShape(shpRecord);
    return value.getShape(geometryFactory);
  }

  public static List<Geometry> readGeometriesWithAttributes(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.SHP_SUFFIX);
    Path dbfFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.DBF_SUFFIX);
    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    FSDataInputStream dbfInputStream = fs.open(dbfFilePath);
    DbfParseUtil dbfParseUtil = new DbfParseUtil();
    dbfParseUtil.parseFileHead(dbfInputStream);
    GeometryFactory geometryFactory = new GeometryFactory();

    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil.readGeometryWithAttributes(shpInputStream, dbfInputStream,
                                                                   shapeFileMeta, dbfParseUtil,
                                                                   geometryFactory));
    }

    return geometries;
  }

  public static List<Geometry> readGeometries(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.SHP_SUFFIX);

    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    GeometryFactory geometryFactory = new GeometryFactory();

    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil.readGeometry(shpInputStream, shapeFileMeta, geometryFactory));
    }

    return geometries;
  }

  public static void saveAsShapefile(String filepath, List<Geometry> geometries, String crs)
      throws IOException, FactoryException {
    File file = new File(filepath);
    file.getParentFile().mkdirs();
    Map<String, Serializable> params = new HashMap<String, Serializable>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
    ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

    SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();

    CoordinateReferenceSystem crsType = CRS.decode(crs);

    tb.setCRS(crsType);

    tb.setName("shapefile");
    tb.add("the_geom", Polygon.class);
    tb.add("outPolyID", Long.class);
    tb.add("minLat", Double.class);
    tb.add("minLon", Double.class);
    tb.add("maxLat", Double.class);
    tb.add("maxLon", Double.class);
    tb.add("att", Double.class);

    ds.createSchema(tb.buildFeatureType());
    ds.setCharset(Charset.forName("GBK"));

    //ReferencingObjectFactory refFactory = new ReferencingObjectFactory();

    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0],
                                                                                 Transaction.AUTO_COMMIT);

    for (int i = 0; i < geometries.size(); i++) {
      SimpleFeature feature = writer.next();
      feature.setAttribute("the_geom", geometries.get(i));
      feature.setAttribute("outPolyID", i);
      Envelope env = geometries.get(i).getEnvelopeInternal();
      feature.setAttribute("minLat", env.getMinY());
      feature.setAttribute("minLon", env.getMinX());
      feature.setAttribute("maxLat", env.getMaxY());
      feature.setAttribute("maxLon", env.getMaxX());
      feature.setAttribute("att", geometries.get(i).getUserData());
    }

    writer.write();
    writer.close();
    ds.dispose();
  }

  public static void saveAsShapefile(String filepath,
                                     String crs,
                                     Class geometryType,
                                     List<Geometry> geometries,
                                     List<Attribute> attributeScheme)
      throws IOException, FactoryException, URISyntaxException {
    /*File file = new File(filepath);
    file.getParentFile().mkdirs();*/
    Configuration hConf = new Configuration();
    URI fsURI = new URI(hConf.get("fs.default.name"));

    FileSystem fs = FileSystem.get(fsURI, hConf);
    Path file = new Path(fsURI + "/" +filepath);
    Path parentDir = file.getParent();

    if (fs.exists(file)) {
      fs.delete(file, true);
    }

    if (!fs.exists(parentDir)) {
      fs.mkdirs(parentDir);
    }


    //OutputStream outputStream = fs.create(file);

    Map<String, Serializable> params = new HashMap<String, Serializable>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toUri().toURL());
    ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

    SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();

    CoordinateReferenceSystem crsType = CRS.decode(crs);
    tb.setCRS(crsType);

    tb.setName("shapefile");

    tb.add("the_geom", geometryType);
    // Sort the attributes by their indices
    Collections.sort(attributeScheme, new Comparator<Attribute>() {
      @Override
      public int compare(Attribute o1, Attribute o2) {
        return o1.getIndex() - o2.getIndex();
      }
    });

    for (Attribute attribute : attributeScheme) {
      tb.add(attribute.getName(), attribute.getType());
    }

    ds.createSchema(tb.buildFeatureType());
    ds.setCharset(Charset.forName("GBK"));

    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0],
                                                                                 Transaction.AUTO_COMMIT);

    boolean hasAttribute = !attributeScheme.isEmpty();
    Iterator<Geometry> geometryIterator = geometries.iterator();
    while (geometryIterator.hasNext()) {
      Geometry geometry = geometryIterator.next();
      SimpleFeature feature = writer.next();

      // populate the geometry
      feature.setAttribute("the_geom", geometry);
      // populate the attributes
      if (hasAttribute) {
        String[] attrs = geometry.getUserData().toString().split("\t");
        //System.out.println(geometry.getUserData().toString());
        for (int i = 0; i < attrs.length; i++) {
          feature.setAttribute(attributeScheme.get(i).getName(), attrs[i]);
        }
      }
    }


    writer.write();
    writer.close();
    ds.dispose();
  }

  public static void saveDbfAsCSV(List<Geometry> geometries,
                                  List<Attribute> attributeSchema,
                                  String csvFilePath)
      throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    for (Attribute attribute : attributeSchema) {
      stringBuilder.append(attribute.getName() + "\t");
    }

    stringBuilder.append("longitude\t");
    stringBuilder.append("latitude\t");
    stringBuilder.append("area");
    stringBuilder.append("\n");

    for (Geometry geometry : geometries) {
      stringBuilder.append(geometry.getUserData());
      stringBuilder.append("\t");
      Point center = geometry.getCentroid();
      stringBuilder.append(center.getX());
      stringBuilder.append("\t");
      stringBuilder.append(center.getY());
      stringBuilder.append("\t");
      stringBuilder.append(geometry.getArea());
      stringBuilder.append("\n");
    }

    Configuration hConf = new Configuration();
    FileSystem fs = FileSystem.get(hConf);
    Path csvFile = new Path(csvFilePath);
    if (fs.exists(csvFile)) {
      fs.delete(csvFile, true);
    }

    OutputStream outputStream = fs.create(csvFile);
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
    bufferedWriter.write(stringBuilder.toString()
                             .replace(",", " ")
                             .replace("\t", ","));
    bufferedWriter.close();
    fs.close();
  }

  public static void saveRows2CSV(String header,
                                  List<String> rows,
                                  String csvFilePath)
      throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(header.replace("\t", ",") + "\n");
    for (String row : rows) {
      stringBuilder.append(row + "\n");
    }

    Configuration hConf = new Configuration();
    FileSystem fs = FileSystem.get(hConf);
    Path csvFile = new Path(csvFilePath);
    if (fs.exists(csvFile)) {
      fs.delete(csvFile, true);
    }

    OutputStream outputStream = fs.create(csvFile);
    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
    bufferedWriter.write(stringBuilder.toString());
    bufferedWriter.close();
    fs.close();
  }
}
