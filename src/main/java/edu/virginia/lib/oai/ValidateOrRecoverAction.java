package edu.virginia.lib.oai;

import nl.mpi.oai.harvester.action.Action;
import nl.mpi.oai.harvester.control.OutputDirectory;
import nl.mpi.oai.harvester.control.Util;
import nl.mpi.oai.harvester.metadata.Metadata;
import nl.mpi.oai.harvester.Provider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.*;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ValidateOrRecoverAction implements Action {

    private static final Logger logger =
            LogManager.getLogger(ValidateOrRecoverAction.class);

    protected OutputDirectory dir;
    protected String suffix;
    protected boolean groupByProvider;

    public ValidateOrRecoverAction(OutputDirectory dir,
                                   String suffix,
                                   boolean groupByProvider) {
        this.dir = dir;
        this.suffix = (suffix == null) ? ".xml.error" : suffix;
        this.groupByProvider = groupByProvider;
    }

    @Override
    public boolean perform(List<Metadata> records) {

        List<Metadata> newRecords = new ArrayList<>();

        for (Metadata record : records) {
            try {

                String originalXml = record.hasDoc()
                        ? serializeDom(record.getDoc())
                        : readStream(record.getStream());

                String id = record.getId();

                List<String> validationErrors = new ArrayList<>();
                boolean isValid = validate(originalXml, validationErrors);

                if (isValid) {
                    newRecords.add(record);
                    continue;
                }

                logger.warn(record.getOrigin().getName() + "Validation failed for record [{}]", id);
                System.out.println(record.getOrigin().getName() + "Validation failed for record "+id);

                List<String> errorMessages = new ArrayList<>(validationErrors);
                String recoveredXml = null;

                if (XmlRecovery.isAvailable()) {

                    XmlRecovery.RecoveryResult result =
                            XmlRecovery.recover(originalXml);

                    for (XmlRecovery.XmlError e : result.errors) {
                        errorMessages.add(formatRecoveryError(e));
                    }

                    recoveredXml = result.xml;

                    if (recoveredXml != null) {
                        logger.info("Recovery succeeded for [{}]", id);
                        System.out.println("Recovery succeeded for "+id);
                    } else {
                        logger.error("Recovery failed for [{}]", id);
                        System.out.println("Recovery failed for "+id);

                    }

                } else {
                    logger.error("libxml2 not available - cannot recover [{}]", id);
                    System.out.println("libxml2 not available - cannot recover "+id);
                }

                // Write diagnostic output using OutputDirectory
                Path outputPath = chooseLocation(record);
                writeDiagnosticFile(outputPath, id,
                        originalXml, recoveredXml, errorMessages);

                if (recoveredXml != null) {

                    Metadata recovered = new Metadata(
                            id,
                            record.getPrefix(),
                            new ByteArrayInputStream(
                                    recoveredXml.getBytes(StandardCharsets.UTF_8)),
                            record.getOrigin(),
                            false,
                            false
                    );

                    newRecords.add(recovered);

                } else {
                    logger.warn("Dropping unrecoverable record [{}]", id);
                    System.out.println("Dropping unrecoverable record "+id);                   
                }

            } catch (Exception ex) {
                logger.error("Fatal error processing [{}]", record.getId(), ex);
                System.err.println("Fatal error processing "+ record.getId());                   
            }
        }

        records.clear();
        records.addAll(newRecords);
        return true;
    }

    /* --------------------------------------------------
       OutputDirectory Integration (SaveAction pattern)
       -------------------------------------------------- */

    protected Path chooseLocation(Metadata metadata) throws IOException {

        if (groupByProvider) {
            Provider prov = metadata.getOrigin();
            OutputDirectory provDir =
                    dir.makeSubdirectory(
                            Util.toFileFormat(prov.getName()));
            return provDir.placeNewFile(
                    Util.toFileFormat(metadata.getId(), suffix));
        }

        return dir.placeNewFile(
                Util.toFileFormat(metadata.getId(), suffix));
    }

    /* --------------------------------------------------
       Diagnostic File Writing
       -------------------------------------------------- */

    private void writeDiagnosticFile(Path file,
                                     String id,
                                     String originalXml,
                                     String recoveredXml,
                                     List<String> errors)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        sb.append("VALIDATION / RECOVERY REPORT\n");
        sb.append("====================================\n");
        sb.append("Identifier: ").append(id).append("\n\n");

        if (errors != null && !errors.isEmpty()) {
            sb.append("Errors:\n");
            for (String err : errors) {
                sb.append(" - ").append(err).append("\n");
            }
            sb.append("\n");
        }

        sb.append("====================================\n\n");
        sb.append("----- ORIGINAL XML -----\n");
        sb.append(originalXml);

        if (recoveredXml != null) {
            sb.append("\n\n----- RECOVERED XML -----\n");
            sb.append(recoveredXml);
        }

        Files.write(file,
                sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /* --------------------------------------------------
       XML Validation
       -------------------------------------------------- */

    private boolean validate(String xml, List<String> errors) {

        try {
            DocumentBuilderFactory dbf =
                    DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setValidating(false);

            DocumentBuilder builder = dbf.newDocumentBuilder();

            builder.setErrorHandler(new ErrorHandler() {

                @Override
                public void warning(SAXParseException e) {
                    errors.add(formatParseError("WARNING", e));
                }

                @Override
                public void error(SAXParseException e) {
                    errors.add(formatParseError("ERROR", e));
                }

                @Override
                public void fatalError(SAXParseException e) {
                    errors.add(formatParseError("FATAL", e));
                }
            });

            builder.parse(new ByteArrayInputStream(
                    xml.getBytes(StandardCharsets.UTF_8)));

            for (String err : errors) {
                if (err.startsWith("ERROR")
                        || err.startsWith("FATAL")) {
                    return false;
                }
            }

            return true;

        } catch (Exception e) {
            errors.add("EXCEPTION: " + e.getMessage());
            return false;
        }
    }

    /* -------------------------------------------------- */

    private String serializeDom(Document doc) throws Exception {
        Transformer transformer =
                TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc),
                new StreamResult(writer));
        return writer.toString();
    }

    private String readStream(InputStream in)
            throws IOException {

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(in,
                        StandardCharsets.UTF_8))) {

            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[8192];
            int n;

            while ((n = br.read(buffer)) != -1) {
                sb.append(buffer, 0, n);
            }

            return sb.toString();
        }
    }

    private String formatParseError(String level,
                                    SAXParseException e) {
        return String.format("%s | Line %d, Column %d | %s",
                level,
                e.getLineNumber(),
                e.getColumnNumber(),
                e.getMessage());
    }

    private String formatRecoveryError(XmlRecovery.XmlError e) {
        return String.format("Line %d | Level %d | Code %d | %s",
                e.line,
                e.level,
                e.code,
                e.message);
    }

    @Override
    public Action clone() {
        return new ValidateOrRecoverAction(
                dir, suffix, groupByProvider);
    }

    @Override
    public String toString() {
        return "validate/recover to " + dir +
                (groupByProvider ? " grouped by provider" : "");
    }
}