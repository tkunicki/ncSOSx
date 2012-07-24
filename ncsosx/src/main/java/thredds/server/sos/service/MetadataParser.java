package thredds.server.sos.service;

import java.io.IOException;
import java.io.Writer;
import java.net.URLDecoder;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import thredds.server.sos.util.XMLDomUtils;
import ucar.nc2.dataset.NetcdfDataset;


/**
 * MetadataParser based on EnhancedMetadataService
 * @author: Andrew Bird
 * Date: 2011
 */
public class MetadataParser {

//    private static final Logger _log = Logger.getLogger(MetadataParser.class);
    
    // TODO :  STATIC Variables!   BAD
    private String service;
    private String version;
    private String request;
    private String observedProperty;
    private String offering;
    private String singleEventTime;
    
    //used for the cases where multiple props are selected
    private String[] observedProperties;

    //used for the cases where muliple event times are selected
    private String[] eventTime;

    /**
     * Enhance NCML with Data Discovery conventions elements if not already in place in the metadata.
     *
     * @param dataset NetcdfDataset to enhance the NCML
     * @param writer writer to send enhanced NCML to
     */
    public static void enhance(final NetcdfDataset dataset, final Writer writer, final String query, String threddsURI) {
        (new MetadataParser()).handleRequest(dataset, writer, query, threddsURI);
    }
    
    public void handleRequest(final NetcdfDataset dataset, final Writer writer, final String query, String threddsURI) {
                
        eventTime = null;

        try {

            if (query != null) {
                //if query is not empty
                //set the query params then call on the fly
                String decoded = URLDecoder.decode(query, "UTF-8");
                splitQuery(decoded); // TODO:  Move this up, use encoding from HTTP Header


                //if all the fields are valid ie not null
                if ((service != null) && (request != null) && (version != null)) {
                    //get caps
                    if (request.equalsIgnoreCase("GetCapabilities")) {
                        SOSGetCapabilitiesRequestHandler handler = new SOSGetCapabilitiesRequestHandler(
                                dataset,
                                threddsURI);
                        handler.parseServiceIdentification();
                        handler.parseServiceDescription();
                        handler.parseOperationsMetaData();
                        handler.parseObservationList();
                        writeDocument(handler.getDocument(), writer);
                        handler.finished();
                    } else if (request.equalsIgnoreCase("DescribeSensor")) {
                        writeErrorXMLCode(writer);
                    } else if (request.equalsIgnoreCase("GetObservation")) {
                        SOSGetObservationRequestHandler handler = new SOSGetObservationRequestHandler(
                                dataset,
                                offering,
                                observedProperties,
                                eventTime);
                        handler.parseObservations();
                        writeDocument(handler.getDocument(), writer);
                        handler.finished();
                    } else {
                        writeErrorXMLCode(writer);
                    }

                } //else if the above is not true print invalid xml text
                else {
                    writeErrorXMLCode(writer);
                }
            } else if (query == null) {
                //if the entry is null just print out the get caps xml
//                _log.info("Null query string/params: using get caps");
                SOSGetCapabilitiesRequestHandler handler = new SOSGetCapabilitiesRequestHandler(
                                dataset,
                                threddsURI);
                        handler.parseTemplateXML();
                        handler.parseServiceIdentification();
                        handler.parseServiceDescription();
                        handler.parseOperationsMetaData();
                        handler.parseObservationList();
                        writeDocument(handler.getDocument(), writer);
                        handler.finished();
            }

            //catch
        } catch (Exception e) {
//            _log.error(e);
            Logger.getLogger(MetadataParser.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    private static void writeErrorXMLCode(final Writer writer) throws IOException, TransformerException {
        Document doc = XMLDomUtils.getExceptionDom();
        writeDocument(doc, writer);
    }

    public static void writeDocument(Document dom, final Writer writer) throws TransformerException, IOException {
        DOMSource source = new DOMSource(dom);
        Result result = new StreamResult(writer);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(source, result);
    }

    public void splitQuery(String query) {
        String[] splitQuery = query.split("&");
        service = null;
        version = null;
        request = null;
        observedProperty = null;

        if (splitQuery.length > 2) {
            for (int i = 0; i < splitQuery.length; i++) {
                String parsedString = splitQuery[i];
                String[] splitServiceStr = parsedString.split("=");
                if (splitServiceStr[0].equalsIgnoreCase("service")) {
                    service = splitServiceStr[1];
                } else if (splitServiceStr[0].equalsIgnoreCase("version")) {
                    version = splitServiceStr[1];
                } else if (splitServiceStr[0].equalsIgnoreCase("request")) {
                    request = splitServiceStr[1];
                } else if (splitServiceStr[0].equalsIgnoreCase("observedProperty")) {
                    observedProperty = splitServiceStr[1];
                    if (observedProperty.contains(",")) {
                        observedProperties = observedProperty.split(",");
                    } else {
                        observedProperties = new String[]{observedProperty};
                    }
                } else if (splitServiceStr[0].equalsIgnoreCase("offering")) {
                    //replace all the eccaped : with real ones
                    String temp = splitServiceStr[1];
                    offering = temp.replaceAll("%3A", ":");

                } else if (splitServiceStr[0].equalsIgnoreCase("eventtime")) {

                    singleEventTime = splitServiceStr[1];
                    if (singleEventTime.contains("/")) {
                        eventTime = singleEventTime.split("/");
                    } else {
                        eventTime = new String[]{singleEventTime};
                    }

                }
            }
        }
    }

}
