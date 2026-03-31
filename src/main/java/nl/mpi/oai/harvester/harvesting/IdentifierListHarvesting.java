/*
 * Copyright (C) 2015, The Max Planck Institute for
 * Psycholinguistics.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * A copy of the GNU General Public License is included in the file
 * LICENSE-gpl-3.0.txt. If that file is missing, see
 *
 * <http://www.gnu.org/licenses/>.
 */

package nl.mpi.oai.harvester.harvesting;

import nl.mpi.oai.harvester.Provider;
import nl.mpi.oai.harvester.action.Action;
import nl.mpi.oai.harvester.action.ActionSequence;
import nl.mpi.oai.harvester.action.SaveAction;
import nl.mpi.oai.harvester.control.Configuration.CompareSkipVals;
import nl.mpi.oai.harvester.control.ResourcePool;
import nl.mpi.oai.harvester.cycle.Endpoint;
import nl.mpi.oai.harvester.metadata.MetadataFactory;
import nl.mpi.oai.harvester.utils.DocumentSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.virginia.lib.oai.ValidateOrRecoverAction;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

/**
 * <br> List based identifier harvesting <br><br>
 *
 * This class provides list based harvesting with a concrete verb to base
 * requests on. Because supplies a specific verb, ListIdentifiers, the response
 * processing needed is specific also. Hence the class also implements this
 * processing. <br><br>
 *
 * Since an endpoint might provide a metadata element in different sets, and
 * record harvesting might involve more than one set, a metadata record could
 * be presented to the client more than once. This class provides every record
 * only once. It uses the list provided by the superclass to remove duplicate
 * identifier and prefix pairs.
 *
 *  Note: originally, this class was declared 'final'. With the addition of
 * tests based on Mockito, this qualifier was removed.
 *
 * @author Kees Jan van de Looij (Max Planck Institute for Psycholinguistics)
 */
public class IdentifierListHarvesting extends ListHarvesting
        implements Harvesting {
    
    private static final Logger logger = LogManager.getLogger(
            IdentifierListHarvesting.class);
    
    /**
     * Associate endpoint and prefixes with the protocol
     * 
     * @param oaiFactory the OAI factory
     * @param provider the endpoint to address in the request
     * @param prefixes the prefixes returned by the endpoint
     * @param metadataFactory the metadata factory
     */
    public IdentifierListHarvesting(OAIFactory oaiFactory,
                                    Provider provider, List<String> prefixes,
                                    MetadataFactory metadataFactory, Endpoint endpoint){

        super(oaiFactory, provider, prefixes, metadataFactory, endpoint);
        // supply the superclass with messages specific to requesting identifiers
        message [0] = "Requesting more identifiers of records with prefix ";
        message [1] = "Requesting identifiers of records with prefix ";
        message [2] = "Cannot get identifiers of ";
    }
    
    /**
     * <br> Create a request based on the two parameter ListIdentifiers verb <br><br>
     *
     * This implementation supplies the form of the verb used in a request
     * based on a resumption token. <br><br>
     *
     * @param p1 metadata prefix
     * @param p2 resumption token
     * @return the response to the request
     */
    @Override
    public DocumentSource verb2(String p1, String p2, int timeout) throws
            IOException,
            ParserConfigurationException,
            SAXException,
            TransformerException,
            NoSuchFieldException,
            XMLStreamException {
        return oaiFactory.createListIdentifiers(p1, p2, timeout);
    }

    /**
     * <br> Create a request based on the two parameter ListIdentifiers verb <br><br>
     *
     * This implementation supplies the form of the verb used in the initial
     * request. <br><br>
     *
     * @param p1 endpoint URL
     * @param p2 from date, for selective harvesting
     * @param p3 until date, for selective harvesting
     * @param p4 metadata prefix
     * @param p5 set
     */
    @Override
    public DocumentSource verb5(String p1, String p2, String p3, String p4,
            String p5, int timeout, Path temp) throws
            IOException,
            ParserConfigurationException,
            SAXException,
            TransformerException,
            NoSuchFieldException,
            XMLStreamException {
        return oaiFactory.createListIdentifiers(p1, p2, p3, p4, p5, timeout);
    }
    
    /**
     * <br> Get the resumption token associated with a specific response <br><br>
     *
     * This method implements a resumption token request by invoking the
     * getResumptionToken OCLC library method. <br><br>
     *
     * @return the token
     */
    @Override
    public String getToken (){

        // check for a protocol error
        if (document == null){
            throw new HarvestingException();
        }

        return oaiFactory.getResumptionToken();
    }

    /**
     * <br> Create a list of metadata elements from the response <br><br>
     *
     * This method filters identifiers from the response. The filter is
     * an XPath expression build around the ListIdentifiers element, the
     * element that holds metadata record headers. The identifiers end up
     * in a target list as input to the processResponse method.
     *
     * Note: when listing records without first retrieving their identifiers,
     * the target list keeps track of duplicate records only. In that case, the
     * parseResponse method returns the metadata from a response directly.
     *
     * @return true if the response was processed successfully, false otherwise
     */
    @Override
    public boolean processResponse(DocumentSource document) {
        
        // check for a protocol error
        if (document == null){
            throw new HarvestingException();
        }

        /* The response is in place, and pIndex <= prefixes.size because of
           the invariant established in the AbstractListHarvesting class.
         */
        try {
            /* Try to add the targets in the response to the list. On 
               failure, stop the work on the current prefix.
             */
            nodeList = (NodeList)provider.xpath.evaluate(
                    "//*[(starts-with(local-name(),'identifier') or  starts-with(local-name(),'datestamp')) "
                            + "and parent::*[local-name()='header' "
                            + "and not(@status='deleted')]]/text()",
                    document.getDocument(), XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            // something went wrong when creating the list, try another prefix
            logger.error(e.getMessage(), e);
            logger.info("Cannot create list of identifiers of " +
                    prefixes.get(pIndex) +
                    " records for endpoint " + provider.oaiUrl);
            return false;
        }
        
        // add the identifier and prefix targets into the array
        for (int j = 0; j < nodeList.getLength(); j+=2) {
            String identifier = nodeList.item(j).getNodeValue();
            String datestamp = nodeList.item(j+1).getNodeValue();
            IdPrefix pair = new IdPrefix (identifier, 
                    prefixes.get(pIndex), datestamp);

            /* Try to insert the pair in the list. No problem if it is already
               there.
             */
            if (provider.getIdentifierFilter() == null || provider.getIdentifierFilterMatch(identifier))
                targets.checkAndInsertSorted(pair);
        }
        
        return true;
    }

    /**
     * Return the next metadata element in the list of targets
     *
     * This method returns the next metadata element from the list of targets
     * created by the processResponse method.
     *
     * @return true if the list was parsed successfully, false otherwise
     */
    @Override
    public Object parseResponse() {
        
        // check for protocol errors
        if (targets == null){
            throw new HarvestingException();
        }
        if (tIndex >= targets.size()) {
            throw new HarvestingException();
        }

        // the targets are in place and tIndex points to an element in the list
        IdPrefix pair = targets.get(tIndex);
        tIndex++;
        // get the record for the identifier and prefix
        RecordHarvesting p = new RecordHarvesting(oaiFactory, provider, pair.prefix, pair.identifier, metadataFactory);

        if (! p.request()) {
            // something went wrong
            return null;
        } else {
            DocumentSource document = p.getResponse();
            if (document == null) {
                return null;
            } else {
                if (!p.processResponse(document)) {
                    return null;
                } else {
                    return p.parseResponse();
                }
            }
        }
    }
    
    public Object parseResponseIfNewer(Path pathToFile, Path pathToErrorFile) throws IOException {

        // check for protocol errors
        if (targets == null){
            throw new HarvestingException();
        }
        if (tIndex >= targets.size()) {
            throw new HarvestingException();
        }

        IdPrefix pair = targets.get(tIndex);

        boolean retryError = false;

        if (Files.exists(pathToErrorFile)) {
            try {
                FileTime errorTime = Files.getLastModifiedTime(pathToErrorFile);
                long ageMillis = System.currentTimeMillis() - errorTime.toMillis();
                long oneDayMillis = 24L * 60L * 60L * 1000L;

                if (ageMillis > oneDayMillis) {
                    retryError = true;
                    logger.info("Retrying previously failed record {} (error file older than 1 day)", pair.identifier);
                    System.out.println("Retrying previously failed record "+pair.identifier+" (error file older than 1 day)");
                } else {
                    logger.debug("Skipping forced retry for {} (error file too recent)", pair.identifier);
                }

            } catch (IOException e) {
                logger.warn("Unable to read error file timestamp for {}", pair.identifier, e);
            }
        }

        org.joda.time.format.DateTimeFormatter formatter =
            DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm':'ssZ");

        DateTime dt = formatter.parseDateTime(pair.datestamp);
        DateTime ht = this.endpoint.getHarvestedDate();

        if (dt.isAfter(ht)) {
            this.endpoint.setHarvestedDate(dt);
        }

        CompareSkipVals compareMode = this.provider.isCompareMode();
        boolean localNewer = false;

        try {
            if (!retryError && Files.exists(pathToFile)) {
                BasicFileAttributes attr = Files.readAttributes(pathToFile, BasicFileAttributes.class);
                FileTime ft = attr.lastModifiedTime();

                long localSeconds = ft.toMillis() / 1000;
                long oaiSeconds   = dt.getMillis() / 1000;

                if (localSeconds - 60 > oaiSeconds) {
                    localNewer = true;

                    if (compareMode == CompareSkipVals.SKIP) {
                        logger.debug("Skipping {} (local >= OAI)", pathToFile.toAbsolutePath());
                        tIndex++;
                        return "already exists";
                    } 
                    else if (compareMode == CompareSkipVals.COMPARE) {
                        logger.warn("Local newer than OAI for {}, will compare content", pair.identifier);
                        System.out.println("Local newer than OAI for "+pair.identifier+", will compare content");
                    }
                    else {
                        // IGNORE MODE -- do mothing
                    }
                }
            }
        } catch (IOException ioe) {
            throw ioe;
        }

        byte[] existingBytes = null;

        if (compareMode == CompareSkipVals.COMPARE && localNewer && Files.exists(pathToFile)) {
            existingBytes = Files.readAllBytes(pathToFile);
        }

        long start = System.nanoTime();

        Object result;
        try {
            result = parseResponse();   // this writes the file via SaveAction
        } finally {
            long durationNs = System.nanoTime() - start;
            double durationS = durationNs / 1_000_000_000.0;
            System.out.println(this.provider.getName() + " : " +
                pathToFile.getFileName() + " took " +
                String.format("%.2f", durationS) + " sec");
        }

        // ---- POST-FETCH COMPARISON ----
        if (compareMode == CompareSkipVals.COMPARE && localNewer && existingBytes != null && Files.exists(pathToFile)) {

            byte[] newBytes = Files.readAllBytes(pathToFile);

            if (contentEquals(existingBytes, newBytes)) {
                // identical -> restore original (avoid unnecessary update)
                Files.write(pathToFile, existingBytes);
                logger.debug("No change detected for {}, keeping existing file", pair.identifier);
            } else {
                logger.warn("Content differs for {} even though local file was newer -> overwriting with harvested version",
                        pair.identifier);
                System.out.println("Content differs for "+pair.identifier+" even though local file was newer -> overwriting with harvested version");
            }
        }

        return result;
    }
    
    private boolean contentEquals(byte[] a, byte[] b) {
        if (java.util.Arrays.equals(a, b)) {
            return true;
        }

        try {
            return normalizeXml(a).equals(normalizeXml(b));
        } catch (Exception e) {
            logger.warn("XML normalization failed, falling back to byte compare", e);
            return false;
        }
    }
    
    private String normalizeXml(byte[] data) throws Exception {

        javax.xml.parsers.DocumentBuilderFactory dbf = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setIgnoringComments(true);
        dbf.setCoalescing(true);

        // IMPORTANT: ignore whitespace
        dbf.setIgnoringElementContentWhitespace(true);

        javax.xml.parsers.DocumentBuilder db = dbf.newDocumentBuilder();
        org.w3c.dom.Document doc = db.parse(new java.io.ByteArrayInputStream(data));

        doc.normalizeDocument();

        javax.xml.transform.TransformerFactory tf = javax.xml.transform.TransformerFactory.newInstance();
        javax.xml.transform.Transformer transformer = tf.newTransformer();

        transformer.setOutputProperty(javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(javax.xml.transform.OutputKeys.INDENT, "no");

        java.io.StringWriter writer = new java.io.StringWriter();
        transformer.transform(new javax.xml.transform.dom.DOMSource(doc),
                              new javax.xml.transform.stream.StreamResult(writer));

        return writer.toString().replaceAll(">\\s+<", "><").trim();
    }
    
    public Object parseResponseIfNewer(ActionSequence actions) throws IOException {
        
        // check for protocol errors
        if (targets == null){
            throw new HarvestingException();
        }
        if (tIndex >= targets.size()) {
            throw new HarvestingException();
        }

        // the targets are in place and tIndex points to an element in the list
        IdPrefix pair = targets.get(tIndex);
        
        ResourcePool<Action> firstSaveAction = Scenario.getFirstSaveAction(actions);
        SaveAction saveAction = ((SaveAction)firstSaveAction.get());
        Path pathToFile = saveAction.chooseLocation(this.provider.getName(), pair.identifier);
        firstSaveAction.release(saveAction);
        
        ResourcePool<Action> validateOrRecoverAction = Scenario.getValidateOrRecoverAction(actions);
        ValidateOrRecoverAction recoverAction = ((ValidateOrRecoverAction)validateOrRecoverAction.get());
        Path pathToErrorFile = recoverAction.chooseLocation(this.provider.getName(), pair.identifier);
        validateOrRecoverAction.release(recoverAction);
        
        return (parseResponseIfNewer(pathToFile, pathToErrorFile));
    }

}
