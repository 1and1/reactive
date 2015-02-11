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





import net.oneandone.reactive.sse.SseEvent;

import org.junit.Assert;
import org.junit.Test;





public class SseEventTest {
    
    
    
    @Test
    public void testSSeEventSimple() throws Exception {
        Assert.assertEquals(SseEvent.newEvent().id("1"), SseEvent.newEvent().id("1"));
        Assert.assertEquals(SseEvent.newEvent().id("1").data("data"), SseEvent.newEvent().id("1").data("data"));
        Assert.assertEquals(SseEvent.newEvent().id("1").data("data").event("evt2"), SseEvent.newEvent().id("1").data("data").event("evt2"));
        Assert.assertNotEquals(SseEvent.newEvent().id("1"), SseEvent.newEvent().id("1").data("data").event("evt2"));
    }
}