package edu.virginia.lib.oai;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import javax.xml.stream.*;

import com.ctc.wstx.stax.WstxInputFactory;

import edu.virginia.lib.oai.XmlRecovery.RecoveryResult;
import edu.virginia.lib.oai.XmlRecovery.XmlError;

public class BatchEadRecoveryAnalyzer {

    private static int total = 0;
    private static int repaired = 0;
    private static int failed = 0;

    private static Map<String, Integer> errorTypes = new TreeMap<>();

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage: java edu.virginia.lib.oai.BatchEadRecoveryAnalyzer <errorRootDir>");
            return;
        }

        Path errorRoot = Paths.get(args[0]);
        Path repairedRoot = Paths.get("repaired");
        Path failedRoot = Paths.get("failed");

        Files.createDirectories(repairedRoot);
        Files.createDirectories(failedRoot);

        Files.walk(errorRoot)
                .filter(p -> p.toString().endsWith(".xml.error"))
                .forEach(p -> processFile(p, errorRoot, repairedRoot, failedRoot));

        writeReport(errorRoot.resolve("analysis-report.txt"));

        System.out.println("Analysis complete.");
    }

    private static void processFile(Path file,
                                    Path errorRoot,
                                    Path repairedRoot,
                                    Path failedRoot) {

        total++;
        System.out.println("Processing: " + file);

        try {

            String xml = Files.readString(file);
            String cleaned = preClean(xml);

            Path relative = errorRoot.relativize(file);
            Path providerPath = relative.getParent();

            String newName = file.getFileName()
                    .toString()
                    .replace(".xml.error", ".xml");

            Path repairedDir = repairedRoot.resolve(providerPath);
            Path failedDir = failedRoot.resolve(providerPath);

            Files.createDirectories(repairedDir);
            Files.createDirectories(failedDir);

            // -----------------------------
            // Stage 1 – Strict parse
            // -----------------------------
            if (canParse(cleaned)) {

                Files.writeString(
                        repairedDir.resolve(newName),
                        cleaned
                );

                repaired++;
                return;
            }

            // -----------------------------
            // Stage 2 – libxml2 Recovery
            // -----------------------------
            RecoveryResult result = XmlRecovery.recover(cleaned);

            // Record libxml2 errors
            for (XmlError err : result.errors) {
                classifyLibXmlError(err);
            }

            // Stage 3 – Strict re-parse validation
            if (result.xml != null && canParse(result.xml)) {

                Files.writeString(
                        repairedDir.resolve(newName),
                        result.xml
                );

                repaired++;
                recordError("RECOVERED_BY_LIBXML2");
                return;
            }

            // Still failing
            Files.copy(file,
                    failedDir.resolve(file.getFileName()),
                    StandardCopyOption.REPLACE_EXISTING);

            failed++;

        } catch (Exception e) {
            failed++;
            recordError("IO_ERROR");
            e.printStackTrace();
        }
    }

    // -----------------------------
    // Strict XML parse check (Woodstox)
    // -----------------------------
    private static boolean canParse(String xml) {

        try {

            XMLInputFactory factory = new WstxInputFactory();
            factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

            XMLStreamReader reader =
                    factory.createXMLStreamReader(new StringReader(xml));

            while (reader.hasNext()) {
                reader.next();
            }

            reader.close();
            return true;

        } catch (XMLStreamException e) {
            classifyWoodstoxError(e.getMessage());
            return false;
        }
    }

    // -----------------------------
    // Stage 0 – sanitation
    // -----------------------------
    private static String preClean(String xml) {

        if (xml == null) return "";

        xml = xml.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "");
        xml = xml.replaceAll("&(?!amp;|lt;|gt;|quot;|apos;|#)", "&amp;");

        return xml;
    }

    // -----------------------------
    // libxml2 error classification
    // -----------------------------
    private static void classifyLibXmlError(XmlError err) {

        String msg = err.message.toLowerCase();

        if (msg.contains("mismatch"))
            recordError("CROSSED_TAG");
        else if (msg.contains("expected") && msg.contains(">"))
            recordError("RAW_LT");
        else if (msg.contains("attribute"))
            recordError("BAD_ATTRIBUTE");
        else if (msg.contains("premature end"))
            recordError("EOF");
        else
            recordError("OTHER");
    }

    // -----------------------------
    // Woodstox error classification
    // -----------------------------
    private static void classifyWoodstoxError(String message) {

        if (message == null) {
            recordError("UNKNOWN");
            return;
        }

        if (message.contains("Unexpected close tag"))
            recordError("CROSSED_TAG");
        else if (message.contains("Unexpected character '<'"))
            recordError("RAW_LT");
        else if (message.contains("expected '='"))
            recordError("BAD_ATTRIBUTE");
        else if (message.contains("unexpected EOF"))
            recordError("EOF");
        else
            recordError("OTHER");
    }

    private static void recordError(String type) {
        errorTypes.put(type, errorTypes.getOrDefault(type, 0) + 1);
    }

    // -----------------------------
    // Report writer
    // -----------------------------
    private static void writeReport(Path reportPath) throws IOException {

        try (PrintWriter out = new PrintWriter(reportPath.toFile())) {

            out.println("===== EAD Recovery Analysis =====");
            out.println("Total files: " + total);
            out.println("Repaired: " + repaired);
            out.println("Failed: " + failed);
            out.println();

            out.println("Error breakdown:");
            for (String key : errorTypes.keySet()) {
                out.println(key + ": " + errorTypes.get(key));
            }
        }
    }
}
