package edu.virginia.lib.oai;

import com.sun.jna.*;
import java.util.Arrays;
import java.util.List;

public interface LibXml2 extends Library {

    LibXml2 INSTANCE = Native.load("xml2", LibXml2.class);

    int XML_PARSE_RECOVER = 1;

    Pointer xmlReadMemory(byte[] inputBytes,
                          int size,
                          String URL,
                          String encoding,
                          int options);

    void xmlFreeDoc(Pointer doc);

    void xmlFree(Pointer ptr);

    /* ---------- Buffer API ---------- */

    Pointer xmlBufferCreate();

    void xmlBufferFree(Pointer buffer);

    Pointer xmlBufferContent(Pointer buffer);

    int xmlBufferLength(Pointer buffer);

    Pointer xmlDocGetRootElement(Pointer doc);

    void xmlNodeDump(Pointer buffer,
                     Pointer doc,
                     Pointer node,
                     int level,
                     int format);

    /* ---------- Error Handling ---------- */

    void xmlSetStructuredErrorFunc(Pointer ctx,
                                   StructuredErrorHandler handler);

    interface StructuredErrorHandler extends Callback {
        void apply(Pointer userData, XmlErrorStruct error);
    }

    class XmlErrorStruct extends Structure {

        public int domain;
        public int code;
        public Pointer message;
        public int level;
        public Pointer file;
        public int line;
        public Pointer str1;
        public Pointer str2;
        public Pointer str3;
        public int int1;
        public int int2;
        public Pointer ctxt;
        public Pointer node;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(
                    "domain", "code", "message", "level",
                    "file", "line", "str1", "str2", "str3",
                    "int1", "int2", "ctxt", "node"
            );
        }
    }
}