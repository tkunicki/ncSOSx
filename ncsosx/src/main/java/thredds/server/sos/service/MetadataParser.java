package thredds.server.sos.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import thredds.server.sos.util.XMLDomUtils;
import ucar.nc2.dataset.NetcdfDataset;


/**
 * MetadataParser based on EnhancedMetadataService
 * @author: Andrew Bird
 * Date: 2011
 */
public class MetadataParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataParser.class);
    
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
                        handleGetCapabilities(dataset, threddsURI, writer);
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
                LOGGER.info("Empty SOS query, assuming GetCapabilities");
                handleGetCapabilities(dataset, threddsURI, writer);
            }

            //catch
        } catch (Exception e) {
            LOGGER.error("Exception handling SOS request: " + e.getMessage(), e);
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
    

    private final Map<File, Object> fileLockMap = Collections.synchronizedMap(new WeakHashMap<File, Object>());
    private void handleGetCapabilities(NetcdfDataset dataset, String tdsURI, Writer outputWriter) throws IOException, TransformerException  {
        
        String ncPath = dataset.getLocation();
        File ncFile = new File(ncPath);
        File ncCacheDirectory = new File(ncFile.getParentFile(), ".ncsosx.cache");
      
        File gcFile = new File(ncCacheDirectory, ncFile.getName() + ".sos.1.capabilities.xml");
        
        Object gcFileLock;
        synchronized (fileLockMap) {
            gcFileLock = fileLockMap.get(gcFile);
            if (gcFileLock == null) {
                gcFileLock = new Object();
                fileLockMap.put(gcFile, gcFileLock);
            }
        }
        
        synchronized(gcFileLock) {
            if (!(ncFile.lastModified() < gcFile.lastModified())) {
                if (!ncCacheDirectory.exists()) {
                    ncCacheDirectory.mkdirs();
                    LOGGER.info("Generated SOS cache directory: " + ncCacheDirectory);
                }
                SOSGetCapabilitiesRequestHandler handler =
                        new SOSGetCapabilitiesRequestHandler(dataset, tdsURI);
                handler.parseTemplateXML();
                handler.parseServiceIdentification();
                handler.parseServiceDescription();
                handler.parseOperationsMetaData();
                handler.parseObservationList();
                Writer cacheWriter = new BufferedWriter(new FileWriter(gcFile));
                try {
                    writeDocument(handler.getDocument(), cacheWriter);
                    IOUtils.closeQuietly(cacheWriter);
                    LOGGER.info("Generated cached GetCapabilities for {}" + ncPath);
                } catch (IOException e) {
                    IOUtils.closeQuietly(cacheWriter);
                    FileUtils.deleteQuietly(gcFile);
                    throw e;
                } catch (TransformerException e) {
                    IOUtils.closeQuietly(cacheWriter);
                    FileUtils.deleteQuietly(gcFile);
                    throw e;
                } finally {
                    if (handler != null) {
                        handler.finished();
                    }
                }
            } else {
                LOGGER.info("Using cached GetCapabilities for {}" + ncPath);
            }
        }
        // TODO:  above code should generate cached document with TDS endpoint as
        // key then search/replace output with actual value used for request.
        Reader cacheReader = new FileReader(gcFile);
        try {
            IOUtils.copy(cacheReader, outputWriter);
        } finally {
            IOUtils.closeQuietly(cacheReader);
        }
    }

}
