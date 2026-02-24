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

package nl.mpi.oai.harvester.control;

import nl.mpi.oai.harvester.Provider;
import nl.mpi.oai.harvester.cycle.Cycle;
import nl.mpi.oai.harvester.cycle.CycleFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;


/**
 * Executable class, main entry point of OAI Harvester.
 *
 * @author Lari Lampen (MPI-PL)
 */
public class Main {
    private static final String sep = System.getProperty("file.separator");
    private static final Logger logger = LogManager.getLogger(Main.class);

    /** Object containing entries from configuration file. */
    public static Configuration config;

    private static void runHarvesting(Configuration config) {
        runHarvesting(config, null); // delegate to the new overload
    }

    private static void runHarvesting(Configuration config, String providerName) {
        config.log();

        ExecutorService executor = new ScheduledThreadPoolExecutor(config.getMaxJobs());

        // create a CycleFactory
        CycleFactory factory = new CycleFactory();
        // get a cycle based on the overview file
        File OverviewFile = new File(config.getOverviewFile());
        Cycle cycle = factory.createCycle(OverviewFile);

        for (Provider provider : config.getProviders()) {
            // If a provider name was specified, skip others
            if (providerName != null && !provider.getName().equals(providerName)) {
                continue;
            }

            // create a new worker
            Worker worker = new Worker(provider, config, cycle);
            executor.execute(worker);
        }

        executor.shutdown();
    }
    public static void main(String[] args) {
        
        logger.info("Welcome to the main OAI Harvest Manager!");
        
        String configFile = null;
        String providerName = null;  // NEW: optional provider argument

        // Select Saxon XSLT/XPath implementation
        System.setProperty("javax.xml.transform.TransformerFactory",    
            "net.sf.saxon.TransformerFactoryImpl");
        System.setProperty("javax.xml.xpath.XPathFactory",
            "net.sf.saxon.xpath.XPathFactoryImpl");

        System.setProperty("http.agent",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");

        // Parse arguments
        for (String arg : args) {
            if (arg.startsWith("config=")) {
                configFile = arg.substring(7);
            } else if (arg.startsWith("provider=")) {   // NEW: capture provider
                providerName = arg.substring(9);
            } else if (!arg.contains("=") && configFile == null) {
                configFile = arg;
            } else if (!arg.contains("=") && configFile != null) {   // NEW: capture provider
                providerName = arg;
            }
        }

        if (configFile == null) {
            configFile = "resources" + sep + "config.xml";
        }

        // Process options given on the command line
        config = new Configuration();
        for (String arg : args) {
            if (arg.indexOf('=') > -1) {
                String[] tmp = arg.split("=");
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

        config.applyTimeoutSetting();
        
        SSLFix.execute();

        // Pass the provider name if specified
        if (providerName != null) {
            runHarvesting(config, providerName);
        } else {
            runHarvesting(config);
        }
        
        logger.info("Goodbye from the main OAI Harvest Manager!");
    }

}
