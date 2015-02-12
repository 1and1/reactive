/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.sse;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;



/**
 * ServerSentEvent parser
 */
public class ServerSentEventParser {
    private final List<String> lines = Lists.newArrayList(); 
    private final LineParser lineParser = new LineParser();
    
    private String id = null;
    private String event = null;
    private String data = "";
    private Integer retry = null;
    
    private String field = ""; 
    private String value = "";
    
        
 
    /**
     * parse the events 
     * 
     * @param buf      the buffer
     * @return the parsed events  
     */
    public ImmutableList<ServerSentEvent> parse(ByteBuffer buf) {
        List<ServerSentEvent> events = Lists.newArrayList();
        
        lineParser.parse(buf, lines);
        
        if (!lines.isEmpty()) {
            for (String line : lines) {
                String trimmedLine = line.trim();
                
                // If the line is empty (a blank line)
                if (trimmedLine.length() == 0) {
                    
                    // If the data buffer's last character is a U+000A LINE FEED (LF) character,
                    // then remove the last character from the data buffer.
                    if (data.endsWith("\n")) {
                        data = data.substring(0, data.length() - 1);
                    }

                    // If the data buffer is an empty string, set the data buffer and the event type 
                    // buffer to the empty string and abort these steps.

                    if ((data.length() > 0 ) || (id != null) || (event != null)) {
                        events.add(ServerSentEvent.newEvent()
                                           .id(id)
                                           .event(event)
                                           .data(data)
                                           .retry(retry));
                    }
                    
                    resetEventParsingData();
                    
                } else {

                    // line starts with a U+003A COLON character (:)
                    // Ignore the line

                    if (!trimmedLine.startsWith(":")) {
                        int idx = line.indexOf(":");
                        
                        // is not empty but does not contain a U+003A COLON character (:)
                        // using the whole line as the field name, and the empty string as the field value.
                        if (idx == -1) {
                            field = line;
                            value = "";
                            
                        } else {
                            // Collect the characters on the line before the first U+003A COLON character (:),
                            // and let field be that string.
                            field += line.substring(0, idx);
                            value = line.substring(idx + 1 , line.length());
                            
                            //  If value starts with a U+0020 SPACE character, remove it from value.
                            value = CharMatcher.is('\u0020').trimLeadingFrom(value);
                            
                            
                            // if the field name is "id" -> Set the last event ID buffer to the field value.
                            String trimmedField = field.trim();
                            if (trimmedField.equalsIgnoreCase("id")) {
                                id = value;
                                        
                            // If the field name is "event" -> Set the event type buffer to field value.
                            } else if (trimmedField.equalsIgnoreCase("event")) { 
                                event = value;
                                
                            // If the field name is "data"-> Append the field value to the data buffer, then
                            // append a single U+000A LINE FEED (LF) character to the data buffer.
                            } else if (trimmedField.equalsIgnoreCase("data")) {
                                data += value + '\n';
                            
                            // If the field value consists of only ASCII digits, then interpret the field 
                            // value as an integer in base ten, and set the event stream's reconnection time to that integer.
                            // Otherwise, ignore the field.
                            } else if (trimmedField.equalsIgnoreCase("retry")) {
                                try {
                                    retry = Integer.getInteger(value);
                                } catch (NumberFormatException ignore) { }
                            }
                            
                            resetFieldValueParsingData();
                        }
                    }
                }
            }
            
            lines.clear();
        }
        
        return ImmutableList.copyOf(events);
    }
    

    private void resetEventParsingData() {
        id = null;
        event = null;
        data = "";
        
        resetFieldValueParsingData();
    }
    
    private void resetFieldValueParsingData() {
        field = "";
        value = "";
    }

    
    
    
    /**
     * @author grro
     */
    private static final class LineParser {
        
        private static final int BUFFER_SIZE = 100; 
        private static final byte CR = 0x0D;
        private static final byte LF = 0x0A;
        

        private byte[] lineBuffer = new byte[BUFFER_SIZE];
        private int pos = 0;
        private boolean isIgnoreLF = false;

        
        
        void parse(ByteBuffer buf, List<String> lines) {
            
            try {
                while (buf.hasRemaining()) {
                    
                    byte i = buf.get();
                    
                    
                    if (i == CR) {    // end-of-line   = ( cr lf / cr / lf )
                        isIgnoreLF = true;
    
                        // new line
                        lines.add(new String(lineBuffer, 0, pos, "UTF-8"));
                        pos = 0;
                        
                    } else if (i == LF) {
                        
                        if (!isIgnoreLF) {
                            // new line
                            lines.add(new String(lineBuffer, 0, pos, "UTF-8"));
                            pos = 0;
                        }
                        
                        isIgnoreLF = false;
                        
                    } else {
                        isIgnoreLF = false;
                        
                        lineBuffer[pos] = i;
                        pos++;
                        
                        incLineBufferIfNecessary();
                    }
                }
            } catch (UnsupportedEncodingException use) {
                throw new RuntimeException(use);
            }
        }
        
        
        private void incLineBufferIfNecessary() {
            if (pos == lineBuffer.length) {
                byte[] newLineBuffer = new byte[lineBuffer.length + BUFFER_SIZE];
                System.arraycopy(lineBuffer, 0, newLineBuffer, 0, lineBuffer.length);
                lineBuffer = newLineBuffer;
            }
        }
    }   
}
