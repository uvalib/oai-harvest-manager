package edu.virginia.lib.oai;

import com.sun.jna.*;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Safe XML recovery using libxml2 via JNA.
 * - Uses XML_PARSE_RECOVER
 * - Collects structured errors
 * - Avoids InvalidMemoryAccess by proper memory handling
 */
public class XmlRecovery {

    private static boolean available = false;
    private static final Logger logger =
            LogManager.getLogger(ValidateOrRecoverAction.class);

    static {
        try {
            if (LibXml2.INSTANCE != null) {
                available = true;
                logger.info("libxml2 loaded successfully.");
            }
        } catch (Throwable t) {
            available = false;
            logger.warn("libxml2 NOT available. Recovery disabled.");
        }
    }

    public static boolean isAvailable() {
        return available;
    }

    /**
     * Attempt to recover XML.
     *
     * @param xmlInput the input XML string
     * @return a RecoveryResult containing recovered XML and errors
     */
    public static RecoveryResult recover(String xmlInput) {

        if (!available) {
            return new RecoveryResult(xmlInput, new ArrayList<>());
        }

        LibXml2 lib = LibXml2.INSTANCE;
        List<XmlError> errors = new ArrayList<>();

        // Register structured error handler
        lib.xmlSetStructuredErrorFunc(null, (userData, errorStruct) -> {
            errorStruct.read();
            String message = errorStruct.message == null ? "" : errorStruct.message.getString(0);
            errors.add(new XmlError(
                    errorStruct.level,
                    errorStruct.code,
                    errorStruct.line,
                    errorStruct.int2,
                    message.trim()
            ));
        });

        // Convert string to UTF-8 bytes to safely pass to native libxml2
        byte[] inputBytes = xmlInput.getBytes(StandardCharsets.UTF_8);

        Pointer doc = lib.xmlReadMemory(
                inputBytes,
                inputBytes.length,
                null,
                "UTF-8",
                LibXml2.XML_PARSE_RECOVER
        );

        if (doc == null) {
            return new RecoveryResult(null, errors);
        }

        Pointer buffer = lib.xmlBufferCreate();
        lib.xmlNodeDump(buffer, doc, lib.xmlDocGetRootElement(doc), 0, 1);

        Pointer content = lib.xmlBufferContent(buffer);
        int len = lib.xmlBufferLength(buffer);

        byte[] bytes = content.getByteArray(0, len);
        String recoveredXml = new String(bytes, StandardCharsets.UTF_8);

        lib.xmlBufferFree(buffer);

        lib.xmlFreeDoc(doc);        
        
        return new RecoveryResult(recoveredXml, errors);
    }


    /**
     * Result of XML recovery
     */
    public static class RecoveryResult {
        public final String xml;
        public final List<XmlError> errors;

        public RecoveryResult(String xml, List<XmlError> errors) {
            this.xml = xml;
            this.errors = errors;
        }
    }

    /**
     * Single XML error captured during recovery
     */
    public static class XmlError {
        public final int level;
        public final int code;
        public final int line;
        public final int int2;
        public final String message;

        public XmlError(int level, int code, int line, int int2, String message) {
            this.level = level;
            this.code = code;
            this.line = line;
            this.int2 = int2;
            this.message = message;
        }
    }
}
