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
 * <http://www.gnu.org/licenses/>.
 */

package nl.mpi.oai.harvester;

import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * Executable class, main entry point of OAI Harvester.
 *
 * @author Lari Lampen (MPI-PL)
 */
public class Main {
    private static final String sep = System.getProperty("file.separator");
    private static final Logger logger = Logger.getLogger(Main.class);

    /** Object containing entries from configuration file. */
    private static Configuration config;

    private static void runHarvesting(Configuration config) {
	config.log();

	// Start a new worker thread for each provider. The Worker class
	// is responsible for honouring the configured limit of
	// concurrent worker threads.
	Worker.setConcurrentLimit(config.getMaxJobs());
	for (Provider prov : config.getProviders()) {
	    Worker worker = new Worker(prov, config.getActionSequences());
	    worker.startWorker();
	}
    }

    public static void main(String[] args) {
	String configFile = null;

	// Select Saxon XSLT/XPath implementation (necessary in case there
    // are other XSLT/XPath libraries in classpath).
    System.setProperty("javax.xml.transform.TransformerFactory",    
        "net.sf.saxon.TransformerFactoryImpl");
    System.setProperty("javax.xml.xpath.XPathFactory",
        "net.sf.saxon.xpath.XPathFactoryImpl");

	// If the "config" parameter is specified, take it as the
	// configuration file name.
	for (String arg : args) {
	    if (arg.startsWith("config=")) {
		configFile = arg.substring(7);
	    } else if (!arg.contains("=")) {
		configFile = arg;
	    }
	}

	// If a configuration file name hasn't been specified in the
	// arguments, use "resources/config.xml" by default.
	if (configFile == null) {
	    configFile = "resources" + sep + "config.xml";
	}

	// Process options given on the command line (if any), then read the
	// configuration file. 
	config = new Configuration();
	for (String arg : args) {
	    if (arg.indexOf('=') > -1) {
		String[] tmp=arg.split("=");
		if (tmp.length == 1) {
		    config.setOption(tmp[0], null);
		} else if (tmp.length >= 2) {
		    config.setOption(tmp[0], tmp[1]);
		}
	    }
	}
	try {
	    config.readConfig(configFile);
	} catch (ParserConfigurationException | SAXException 
		| XPathExpressionException | IOException ex) {
	    logger.error("Unable to read configuration file", ex);
	    return;
	}

	// Ensure the timeout setting is honored.
	config.applyTimeoutSetting();

	runHarvesting(config);
    }
}
