/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.manifest.codegen.runtime.customs.generic;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomRecordExtractor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Java port of {@code source_rss.components.CustomExtractor}.
 *
 * <p>Python source: airbyte-integrations/connectors/source-rss/source_rss/components.py
 * (extract_records method). Handles both RSS 2.0 ({@code <item>}) and Atom 1.0
 * ({@code <entry>}) feeds.
 */
public final class RssCustomExtractor implements CustomRecordExtractor {

    @SuppressWarnings("unused")
    public RssCustomExtractor(Map<String, String> connectorConfig, Map<String, Object> params) {
    }

    /** Delegates to extractFromRawBody — XML cannot be represented as JsonNode. */
    @Override
    public List<Map<String, Object>> extract(JsonNode response) {
        return List.of();
    }

    @Override
    public List<Map<String, Object>> extractFromRawBody(String body) {
        if (body == null || body.isBlank()) return List.of();
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setNamespaceAware(true);
            Document doc = dbf.newDocumentBuilder()
                .parse(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
            doc.getDocumentElement().normalize();

            // Detect RSS vs Atom by root element local name
            String rootName = doc.getDocumentElement().getLocalName();
            if ("feed".equalsIgnoreCase(rootName)) {
                return parseAtom(doc);
            } else {
                return parseRss(doc);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse RSS/Atom feed", e);
        }
    }

    private List<Map<String, Object>> parseRss(Document doc) {
        List<Map<String, Object>> records = new ArrayList<>();
        NodeList items = doc.getElementsByTagNameNS("*", "item");
        if (items.getLength() == 0) {
            items = doc.getElementsByTagName("item");
        }
        for (int i = 0; i < items.getLength(); i++) {
            if (!(items.item(i) instanceof Element el)) continue;
            Map<String, Object> record = new LinkedHashMap<>();
            putText(record, "title", el, "title");
            putText(record, "link", el, "link");
            putText(record, "description", el, "description");
            putText(record, "author", el, "author");
            putText(record, "category", el, "category");
            putText(record, "comments", el, "comments");
            putText(record, "guid", el, "guid");
            // enclosure is an element with attributes
            NodeList encEls = el.getElementsByTagName("enclosure");
            if (encEls.getLength() > 0 && encEls.item(0) instanceof Element enc) {
                record.put("enclosure", enc.getAttribute("url"));
            }
            // pubDate → published in ISO 8601
            String pub = textOf(el, "pubDate");
            if (pub == null) pub = textOf(el, "pubdate");
            record.put("published", toIso(pub));
            records.add(record);
        }
        return records;
    }

    private List<Map<String, Object>> parseAtom(Document doc) {
        List<Map<String, Object>> records = new ArrayList<>();
        NodeList entries = elementsByName(doc, "entry");
        for (int i = 0; i < entries.getLength(); i++) {
            if (entries.item(i) instanceof Element el) {
                records.add(atomEntryToRecord(el));
            }
        }
        return records;
    }

    private Map<String, Object> atomEntryToRecord(Element el) {
        Map<String, Object> record = new LinkedHashMap<>();
        putText(record, "title", el, "title");
        putAtomLink(record, el);
        putAtomDescription(record, el);
        putAtomAuthor(record, el);
        putText(record, "guid", el, "id");
        String pub = textOf(el, "published");
        if (pub == null) pub = textOf(el, "updated");
        record.put("published", toIso(pub));
        return record;
    }

    private static void putAtomLink(Map<String, Object> record, Element el) {
        NodeList links = el.getElementsByTagNameNS("*", "link");
        if (links.getLength() > 0 && links.item(0) instanceof Element linkEl) {
            String href = linkEl.getAttribute("href");
            if (!href.isBlank()) record.put("link", href);
        }
    }

    private static void putAtomDescription(Map<String, Object> record, Element el) {
        String desc = textOf(el, "summary");
        if (desc == null) desc = textOf(el, "content");
        if (desc != null) record.put("description", desc);
    }

    private static void putAtomAuthor(Map<String, Object> record, Element el) {
        NodeList authors = el.getElementsByTagNameNS("*", "author");
        if (authors.getLength() > 0 && authors.item(0) instanceof Element authEl) {
            String name = textOf(authEl, "name");
            if (name != null) record.put("author", name);
        }
    }

    private static NodeList elementsByName(Document doc, String name) {
        NodeList nl = doc.getElementsByTagNameNS("*", name);
        return nl.getLength() > 0 ? nl : doc.getElementsByTagName(name);
    }

    private static void putText(Map<String, Object> map, String key, Element parent, String tagName) {
        String val = textOf(parent, tagName);
        if (val != null) map.put(key, val);
    }

    private static String textOf(Element parent, String tagName) {
        NodeList nl = parent.getElementsByTagNameNS("*", tagName);
        if (nl.getLength() == 0) nl = parent.getElementsByTagName(tagName);
        if (nl.getLength() == 0) return null;
        Node n = nl.item(0);
        // Only return text if the first match is a direct child of parent
        if (n.getParentNode() != parent) return null;
        String t = n.getTextContent();
        return (t == null || t.isBlank()) ? null : t.trim();
    }

    private static String toIso(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return null;
        String s = dateStr.trim();
        String result = tryParse(s, DateTimeFormatter.RFC_1123_DATE_TIME);
        if (result == null) result = tryParse(s, DateTimeFormatter.ISO_DATE_TIME);
        if (result == null) {
            result = tryParse(s,
                DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH));
        }
        if (result == null) {
            result = tryParse(s,
                DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss zzz", Locale.ENGLISH));
        }
        return result != null ? result : s;
    }

    private static String tryParse(String s, DateTimeFormatter fmt) {
        try {
            return ZonedDateTime.parse(s, fmt).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
