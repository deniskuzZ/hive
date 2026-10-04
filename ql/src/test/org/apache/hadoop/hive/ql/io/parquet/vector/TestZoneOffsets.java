/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * ZoneOffsets against ZoneRules for every zone: either side of each transition from 1800 to 2200, which includes
 * periods with two transitions, and random instants in that span and across the range of INT64 milliseconds, in random
 * order so slots are filled and replaced out of order.
 */
public class TestZoneOffsets {

  @Test
  public void testMatchesZoneRules() {
    Random rnd = new Random(7);
    long from = LocalDateTime.parse("1800-01-01T00:00:00").toEpochSecond(ZoneOffset.UTC);
    long to = LocalDateTime.parse("2200-01-01T00:00:00").toEpochSecond(ZoneOffset.UTC);
    long maxMillisSecond = Long.MAX_VALUE / 1000;
    for (String id : ZoneId.getAvailableZoneIds()) {
      ZoneRules rules = ZoneId.of(id).getRules();
      List<Long> instants = new ArrayList<>();
      for (ZoneOffsetTransition t = rules.nextTransition(Instant.ofEpochSecond(from));
          t != null && t.toEpochSecond() < to; t = rules.nextTransition(t.getInstant())) {
        instants.addAll(List.of(t.toEpochSecond() - 1, t.toEpochSecond(), t.toEpochSecond() + 1));
      }
      for (int i = 0; i < 1000; i++) {
        instants.add(from + (long) (rnd.nextDouble() * (to - from)));
        instants.add((long) ((rnd.nextDouble() * 2 - 1) * maxMillisSecond));
      }
      Collections.shuffle(instants, rnd);
      ZoneOffsets offsets = new ZoneOffsets(rules);
      for (long s : instants) {
        assertEquals(id + " at " + s, rules.getOffset(Instant.ofEpochSecond(s)).getTotalSeconds(), offsets.seconds(s));
      }
    }
  }
}
