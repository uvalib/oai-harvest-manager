/*
 * Copyright (C) 2014, The Max Planck Institute for
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

package nl.mpi.oai.harvester;

import ORG.oclc.oai.harvester2.verb.HarvesterVerb;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * List oriented application of the protocol.
 * 
 * Describe the flow. Clearly explain what the target list is for: records that
 * are available to the client. There is a difference in implementation. Listing
 * identifiers, all the identifiers are stored first, before retrieving each
 * record individually. When listing records, the list is used to determine 
 * whether or not a record is a duplicate.
 * 
 * @author keeloo
 */
public abstract class ListProtocol implements Protocol {

    private static final Logger logger = Logger.getLogger(ListProtocol.class);
    
    // messages specific to extending classes
    protected static String[] message = new String [3];

    // information on where to send the request
    protected final Provider provider;
    // pointer to current set
    protected int sIndex;
    
    // metadata prefixes that need to be requested
    protected final List<String> prefixes;
    // pointer to current prefix
    protected int pIndex;
    
    // kj: list of response elements 
    protected NodeList nodeList;
    // pointer to next element that needs to be checked 
    protected int nIndex;   
    
    // the resumption token send by the previous request
    protected String resumptionToken;
    
    // kj: 
    protected SortedArrayList <IdPrefix> targets;
    // pointer to next element to be parsed and returned
    protected int tIndex;  
    
    /**
     * ArrayList sorted according a to the relation defined on the elements
     * 
     * @param <T> the type of the elements in the list
     */
    protected class SortedArrayList<T> extends ArrayList<T> {
        
        /**
         * Insert an element into the list if and only if it is not already 
         * included in the list.
         * 
         * @param element he element to be inserted
         * @return true if the element was inserted, false otherwise
         */
        public boolean checkAndInsertSorted(T element) {
            
            int i = 0, j;
            Comparable<T> c = (Comparable<T>) element;
            for (;;) {
                if (i == this.size()) {
                    // element not included yet
                    this.add(element); 
                    return true;
                }
                j = c.compareTo(this.get(i));
                if (j == 0) {
                    // found a match, element already in the list
                    return false;
                } else {
                    if (j > 0) {
                        // there could still be a match, continue 
                        i++;
                    } else {
                        // there will not be a match, insert
                        this.add(i, element);
                        return true;
                    }
                }
            }
        }
    }
    
    /**
     * Pair of identifier and prefix. By the compareTo method the class defines
     * an ordering relation on the pairs.
     */
    protected class IdPrefix implements Comparable {

        // constituents of the idPrefix
        final String identifier;
        final String prefix;
        
        IdPrefix (String identifier, String prefix){
            this.identifier = identifier;
            this.prefix     = prefix;
        }
        
        /**
         * Compare the IdPrefix object to another one
         * 
         * @param object  another idPrefix to be compared to the idPrefix object
         * @return  -1 if the parameters is smaller than the object
         *           0 if equal
         *           1 if greater 
         */
        @Override
        public int compareTo (Object object){

            if (!(object instanceof IdPrefix)) {
                // we do not expect this
                return 0;
            } else {
                IdPrefix idPrefix = (IdPrefix) object;

                int cIden = this.identifier.compareTo(idPrefix.identifier);
                int cPref = this.prefix.compareTo(idPrefix.prefix);
                
                if (cIden != 0) {
                    return cIden;
                } else {
                    // identifiers are equal, prefixes will not be
                    return cPref;
                }
            }
        }
    }
    
    /**
     * Create object, associate endpoint data and desired prefix 
     * 
     * @param provider the endpoint to address in the request
     * @param prefixes the prefixes returned by the endpoint 
     */
    public ListProtocol (Provider provider, List<String> prefixes){
        this.provider   = provider;
        this.prefixes   = prefixes;
        pIndex          = 0;
        response        = null;
        nIndex          = 0;
        resumptionToken = null;
        tIndex          = 0;
        targets         = new SortedArrayList <> ();
    }
    
    /**
     * Verb with two string parameters. A subclass needs to make this verb 
     * effective for example by invoking the ListRecords or ListIdentifiers 
     * request method. 
     *
     * @return the response
     */
    abstract HarvesterVerb verb2(String s1, String s2)
            throws 
            IOException,
            ParserConfigurationException,
            SAXException,
            TransformerException,
            NoSuchFieldException;

    /**
     * Verb with five string parameters. A subclass needs to make this verb 
     * effective for example by invoking the ListRecords or ListIdentifiers 
     * request method. 
     *
     * @return the response
     */
    abstract HarvesterVerb verb5(String s1, String s2, String s3, String s4, 
            String s5)
            throws 
            IOException,
            ParserConfigurationException,
            SAXException,
            TransformerException,
            NoSuchFieldException;
    
    /**
     * Get the token indicating more data is available. Since a HarvesterVerb 
     * object does not have a method for getting the token the extending classes 
     * need to make this method effective.
     * 
     * @param  response
     * @return  a string containing the token
     * @throws  TransformerException
     * @throws  NoSuchFieldException 
     */
    abstract String getToken (HarvesterVerb response) throws TransformerException, 
            NoSuchFieldException;

    // response to the request
    HarvesterVerb response; 
    
    /**
     * Request data from endpoint. Iterate over the prefixes supplied, for each 
     * prefix iterate over the sets indicated in the provider object. If all 
     * prefixes and sets have been iterated over and the endpoint does no longer
     * respond with data, the requestMore method will return false.
     * 
     * @return false if an error occurred, true otherwise.
     */
    @Override
    public boolean request() {
        
        // check for protocol errors
        
        if (pIndex >= prefixes.size()) {
            logger.error("Protocol error");
            return false;
        }
        
        if (provider.sets != null){
            if (sIndex >= provider.sets.length){
                logger.error("Protocol error");
                return false;
            }
        }
        
        response = null;
        
        // a new node list for processing the list records request
        // kj: not used in list identifiers
        nIndex = 0;

        try {
            // try to get a response from the endpoint
            if (!(resumptionToken == null || resumptionToken.isEmpty())) {
                // use resumption token
                logger.debug(message[0] + prefixes.get(pIndex));

                response = verb2(provider.oaiUrl, resumptionToken);
            } else {
                logger.debug(message[1] + prefixes.get(pIndex));

                if (provider.sets == null) {
                    // no sets specified, ask for records by prefix
                    response = verb5(provider.oaiUrl, null, null,
                            null,
                            prefixes.get(pIndex));
                } else {
                    // request targets for a new set and prefix combination 
                    response = verb5(provider.oaiUrl, null, null,
                            provider.sets[sIndex],
                            prefixes.get(pIndex));
                }
            }
       
            // check if more records would be available
            resumptionToken = getToken(response);

        } catch ( IOException 
                | ParserConfigurationException 
          | SAXException | TransformerException | NoSuchFieldException e) {
            // something went wrong with the request, try another prefix
            logger.error(e.getMessage(), e);
            if (provider.sets.length == 0) {
                logger.info(message[2] + prefixes.get(pIndex)
                        + " records from endpoint " + provider.oaiUrl);

            } else {
                logger.info(message[2] + prefixes.get(pIndex)
                        + " records in set " + provider.sets[sIndex]
                        + " from endpoint " + provider.oaiUrl);
            }
            return false;
        }
        return true;
    }

    /**
     * Find out if more records would be available 
     * 
     * @return true if in principle there would be, false otherwise
     */
    @Override
    public boolean requestMore() {
        
        if (!(resumptionToken == null || resumptionToken.isEmpty())) {
            // indicate another request could be made 
            return true;
        } else {
            // no need to resume requesting within the current set and prefix
            if (provider.sets == null) {
                pIndex++;
                return pIndex != prefixes.size(); // done
            } else {
                sIndex++;
                if (sIndex == provider.sets.length) {
                    // try the next prefix
                    sIndex = 0;
                    pIndex++;
                    return pIndex != prefixes.size(); // done
                } else {
                    // try the next set
                    logger.debug("Requesting records in the "
                            + provider.sets[sIndex] + " set");
                    return true;
                }
            }
        }
    }
    
    /**
     * Check for more records in the list
     * 
     * @return true if there are more, false otherwise
     */
    @Override
    public boolean fullyParsed() {
        return tIndex == targets.size();
    }
}