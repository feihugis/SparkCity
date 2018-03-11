package edu.gmu.stc.hibernate;

import org.apache.hadoop.fs.Path;
import org.hibernate.Session;
import org.apache.hadoop.conf.Configuration;

import javax.print.Doc;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class TestHibernate {

  public static void main(String[] args) {
    String tableName = "table_shp";
    Configuration configuration = new Configuration();
    configuration.addResource(new Path("/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"));
    PhysicalNameStrategyImpl physicalNameStrategy = new PhysicalNameStrategyImpl(tableName);
    HibernateUtil hibernateUtil = new HibernateUtil();
    hibernateUtil.createSessionFactoryWithPhysicalNamingStrategy(configuration, physicalNameStrategy, ShapeFileMeta.class);
    Session session = hibernateUtil.getSession();

    DAOImpl dao = new DAOImpl();
    dao.setSession(session);
    ShapeFileMeta shapeFileMeta = new ShapeFileMeta(41l, 5, 10l, 10,
                                                    20l, 20, "a/b/c",
                                                    -0.5, -0.5, -0.5, -0.5);
    dao.insertDynamicTableObject(tableName, shapeFileMeta);
    hibernateUtil.closeSession();
    hibernateUtil.closeSessionFactory();

    //HibernateUtil.shutdown();

  }

}
