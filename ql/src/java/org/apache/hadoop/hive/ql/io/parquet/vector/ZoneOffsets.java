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

import java.time.Instant;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Arrays;

/**
 * The offsets of a time zone, cached per period of 2^22 seconds (about 48.5 days): a period keeps its offset at the
 * start and the one transition it may contain. A period with more transitions, rare in the zone rules, is looked up in
 * the rules each time.
 */
final class ZoneOffsets {

  private static final int PERIOD_SHIFT = 22;
  /** About 136 years of periods before two of them share a slot. */
  private static final int SLOTS = 1024;

  private final ZoneRules rules;
  private final long[] periods = new long[SLOTS];
  private final long[] transitions = new long[SLOTS];
  private final int[] before = new int[SLOTS];
  private final int[] after = new int[SLOTS];

  ZoneOffsets(ZoneRules rules) {
    this.rules = rules;
    Arrays.fill(periods, Long.MIN_VALUE);
  }

  /** The offset in seconds at {@code epochSecond}, as {@link ZoneRules#getOffset(Instant)} gives it. */
  int seconds(long epochSecond) {
    long period = epochSecond >> PERIOD_SHIFT;
    int slot = (int) period & (SLOTS - 1);
    if (periods[slot] != period && !cache(slot, period)) {
      return rules.getOffset(Instant.ofEpochSecond(epochSecond)).getTotalSeconds();
    }
    return epochSecond < transitions[slot] ? before[slot] : after[slot];
  }

  private boolean cache(int slot, long period) {
    Instant start = Instant.ofEpochSecond(period << PERIOD_SHIFT);
    long end = (period + 1) << PERIOD_SHIFT;
    ZoneOffsetTransition next = rules.nextTransition(start);
    long transition = Long.MAX_VALUE;
    if (next != null && next.toEpochSecond() < end) {
      ZoneOffsetTransition second = rules.nextTransition(next.getInstant());
      if (second != null && second.toEpochSecond() < end) {
        return false;
      }
      transition = next.toEpochSecond();
      after[slot] = next.getOffsetAfter().getTotalSeconds();
    }
    transitions[slot] = transition;
    before[slot] = rules.getOffset(start).getTotalSeconds();
    periods[slot] = period;
    return true;
  }
}
