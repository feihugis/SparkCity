package edu.gmu.stc.vector.operation;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.httpclient.NameValuePair;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher;
import it.geosolutions.geoserver.rest.GeoServerRESTPublisher.UploadMethod;
import it.geosolutions.geoserver.rest.GeoServerRESTReader;

/*
 * Geoserver manager: https://github.com/geosolutions-it/geoserver-manager/wiki/Various-Examples
 * How to do it through REST in postman: https://gis.stackexchange.com/questions/12970/create-a-layer-in-geoserver-using-rest
 * http://docs.geoserver.org/latest/en/user/gettingstarted/shapefile-quickstart/index.html
 * 1. Upload data (any local file is fine, zip)
 * 2. Create workspace (can be done manually)
 * 3. Create data store (optional)
 * 4. Publish layer
 */
public class Geoclient {

  private final String RESTURL  = "http://199.26.254.146:8080/geoserver";
  private final String RESTUSER = "admin";
  private final String RESTPW   = "geoserver";
  GeoServerRESTReader reader;
  GeoServerRESTPublisher publisher;

  public Geoclient() {
    try {
      reader = new GeoServerRESTReader(RESTURL, RESTUSER, RESTPW);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    publisher = new GeoServerRESTPublisher(RESTURL, RESTUSER, RESTPW);
  }

  public boolean createWorkspace(String workspace_name)
  {
    return publisher.createWorkspace(workspace_name);
  }

  /*
   * WMS_url_pattern:
   * http://localhost:8080/geoserver/$workspace_name/wms?service=WMS&version=1.1.0&request=GetMap&layers=$workspace_name:$data_store_name&styles=&bbox=$bbox&width=506&height=768&srs=$proj&format=application/openlayers
   * http://localhost:8080/geoserver/javatest/wms?service=WMS&version=1.1.0&request=GetMap&layers=javatest:nyc_roads&styles=&bbox=984018.1663741902,207673.09513056703,991906.4970533887,219622.53973435296&width=506&height=768&srs=EPSG:2908&format=application/openlayers
   */
  public String publishShapefile(String workspace_name, String data_store_name, String datasetname,
                                  String remote_data_path, String proj, String bbox)
  {
    String publishedUrl = "";
    boolean bPublished = false;

    this.createWorkspace(workspace_name);

    URI uri;
    try {
      uri = new URI(remote_data_path);
      bPublished = publisher.publishShp(workspace_name, data_store_name, new NameValuePair[0], datasetname,
              UploadMethod.EXTERNAL, uri, proj, null);
    } catch (Exception e) {
      e.printStackTrace();
    }

    if(bPublished){
      publishedUrl = this.RESTURL  + "/" + workspace_name +"/wms?service=WMS&version=1.1.0&request=GetMap&layers=" + workspace_name + ":" + datasetname +"&styles=&bbox=" + bbox + "&width=506&height=768&srs=" + proj + "&format=application/openlayers";
    }
    return publishedUrl;
  }

  public static void main(String[] args) throws URISyntaxException {
    Geoclient client = new Geoclient();
    //System.out.println(client.createWorkspace("yun"));
    System.out.println(client.publishShapefile("yun", "yuntest", "nyc_roads", "file:////usr/share/geoserver/data_dir/data/nyc_roads/nyc_roads.shp", "EPSG:2908", ""));
    //File zipFile = new File("/Users/yjiang/Downloads/nyc_roads.zip");
//    URI uri = new URI("file:///home/data/nyc_roads/nyc_roads.shp");
//    try {
//      boolean published = client.publisher.publishShpCollection("javatest", "myStore", uri);
//      System.out.println(published);
//    } catch (FileNotFoundException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } catch (IllegalArgumentException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
  }

}