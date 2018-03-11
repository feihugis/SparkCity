package edu.gmu.stc.config;

/**
 * Created by Fei Hu on 1/29/18.
 */
public class ConfigParameter {

  //Hibernate
  public static final String HIBERNATE_DRIEVER = "hibernate.connection.driver_class";  //org.postgresql.Driver
  public static final String HIBERNATE_URL = "hibernate.connection.url";  //jdbc:postgresql://localhost:5432/hibernate_test
  public static final String HIBERNATE_USER = "hibernate.connection.username";
  public static final String HIBERNATE_PASS = "hibernate.connection.password";
  public static final String HIBERNATE_DIALECT = "hibernate.dialect";  //org.hibernate.dialect.PostgreSQL9Dialect
  public static final String HIBERNATE_HBM2DDL_AUTO = "hibernate.hbm2ddl.auto"; //update

  //HDFS
  public static final String INPUT_DIR_PATH = "mapred.input.dir";
  public static final String SHAPEFILE_INDEX_TABLES = "shapefile.index.tablenames";

}
