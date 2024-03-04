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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.IntKey;
import accord.impl.IntKey.Routing;
import accord.primitives.Range;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.LongLongFunction;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class RTreeTest
{
    private static final Logger logger = LoggerFactory.getLogger(RTreeTest.class);
    private static final Comparator<Routing> COMPARATOR = Comparator.naturalOrder();
    private static final RTree.Accessor<Routing, Range> END_INCLUSIVE = new RTree.Accessor<>()
    {
        @Override
        public Routing start(Range range)
        {
            return (Routing) range.start();
        }

        @Override
        public Routing end(Range range)
        {
            return (Routing) range.end();
        }

        @Override
        public boolean contains(Range range, Routing routing)
        {
            return range.contains(routing);
        }

        @Override
        public boolean contains(Routing start, Routing end, Routing routing)
        {
            if (routing.compareTo(start) <= 0)
                return false;
            if (routing.compareTo(end) > 0)
                return false;
            return true;
        }

        @Override
        public boolean intersects(Range range, Routing start, Routing end)
        {
            return range.compareIntersecting(IntKey.range(start, end)) == 0;
        }

        @Override
        public boolean intersects(Range left, Range right)
        {
            return left.compareIntersecting(right) == 0;
        }
    };
    private static final RTree.Accessor<Routing, Range> ALL_INCLUSIVE = new RTree.Accessor<>()
    {
        @Override
        public Routing start(Range range)
        {
            return (Routing) range.start();
        }

        @Override
        public Routing end(Range range)
        {
            return (Routing) range.end();
        }

        @Override
        public boolean contains(Range range, Routing routing)
        {
            return range.contains(routing) || range.start().equals(routing);
        }

        @Override
        public boolean contains(Routing start, Routing end, Routing routing)
        {
            if (routing.compareTo(start) < 0)
                return false;
            if (routing.compareTo(end) > 0)
                return false;
            return true;
        }

        @Override
        public boolean intersects(Range range, Routing start, Routing end)
        {
            return range.compareIntersecting(IntKey.range(start, end)) == 0 || range.end().equals(start) || range.start().equals(end);
        }

        @Override
        public boolean intersects(Range left, Range right)
        {
            return left.compareIntersecting(right) == 0 || left.end().equals(right.start()) || left.start().equals(right.end());
        }
    };

    private static final Gen.IntGen SMALL_INT_GEN = rs -> rs.nextInt(0, 10);
    private static final int MIN_TOKEN = 0, MAX_TOKEN = 1 << 16;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen.IntGen TOKEN_GEN = rs -> rs.nextInt(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Range> RANGE_GEN = rs -> {
        int a = TOKEN_GEN.nextInt(rs);
        int b = TOKEN_GEN.nextInt(rs);
        while (a == b)
            b = TOKEN_GEN.nextInt(rs);
        if (a > b)
        {
            int tmp = a;
            a = b;
            b = tmp;
        }
        return IntKey.range(a, b);
    };

    private enum Pattern
    {RANDOM, NO_OVERLP, SMALL_RANGES}

    @Test
    public void listModel()
    {
        test(true);
    }

    @Test
    public void intervalTreeModel()
    {
        test(false);
    }

    public void test(boolean useList)
    {
        int samples = 10_000;
        Gen<Pattern> patternGen = Gens.enums().all(Pattern.class);
        qt().withExamples(10).check(rs -> {

            var map = create(useList);
            var model = createModel(useList);

            LongArrayList byToken = new LongArrayList(samples, -1);
            LongArrayList modelByToken = new LongArrayList(samples, -1);
            LongArrayList byTokenLength = new LongArrayList(samples, -1);
            LongArrayList byRange = new LongArrayList(samples, -1);
            LongArrayList modelByRange = new LongArrayList(samples, -1);
            LongArrayList byRangeLength = new LongArrayList(samples, -1);
            Pattern pattern = patternGen.next(rs);
            Gen<Range> rangeGen = rangeGen(pattern, samples);
            for (int i = 0; i < samples; i++)
            {
                var range = rangeGen.next(rs);
                var value = SMALL_INT_GEN.nextInt(rs);
                map.add(range, value);
                model.put(range, value);
            }
            model.done();
            Assertions.assertThat(map).hasSize(samples);
            // reset range gen as NO_OVERLAP has state
            rangeGen = rangeGen(pattern, samples);
            for (int i = 0; i < samples; i++)
            {
                {
                    // key lookup
                    var lookup = IntKey.routing(TOKEN_GEN.nextInt(rs));
                    var actual = timed(byToken, () -> map.searchToken(lookup));
                    var expected = timed(modelByToken, () -> model.intersectsToken(lookup));
                    byTokenLength.addLong(expected.size());
                    Assertions.assertThat(sort(actual))
                              .describedAs("Write=%d; token=%s", i, lookup)
                              .isEqualTo(sort(expected));
                }
                {
                    // range lookup
                    var lookup = rangeGen.next(rs);
                    var actual = timed(byRange, () -> map.search(lookup));
                    var expected = timed(modelByRange, () -> model.intersects(lookup));
                    byRangeLength.addLong(expected.size());
                    Assertions.assertThat(sort(actual))
                              .describedAs("Write=%d; range=%s", i, lookup)
                              .isEqualTo(sort(expected));
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("=======");
            sb.append("\nPattern: " + pattern);
            long modelCost = ObjectSizes.measureDeep(model.actual());
            sb.append("\nModel: " + model.actual().getClass().getSimpleName());
            sb.append("\nModel Memory Cost: " + modelCost);
            long actualCost = ObjectSizes.measureDeep(map);
            sb.append("\nMemory Cost: " + actualCost + "; " + String.format("%.2f", ((actualCost - modelCost) / (double) actualCost * 100.0)) + "% model size");
            sb.append("\n=======");
            sb.append("\nBy Token:");
            sb.append("\n\tSizes: " + stats(byTokenLength, false));
            sb.append("\n\tModel: " + stats(modelByToken, true));
            sb.append("\n\tRTree: " + stats(byToken, true));
            sb.append("\nBy Range:");
            sb.append("\n\tSizes: " + stats(byRangeLength, false));
            sb.append("\n\tModel: " + stats(modelByRange, true));
            sb.append("\n\tRTree: " + stats(byRange, true));
            logger.info(sb.toString());
        });
    }

    private static Gen<Range> rangeGen(Pattern pattern, int samples)
    {
        switch (pattern)
        {
            case RANDOM:
                return RANGE_GEN;
            case SMALL_RANGES:
                return random -> {
                    int a = TOKEN_GEN.nextInt(random);
                    int rangeSize = random.nextInt(100, (int) (TOKEN_RANGE_SIZE * .01));
                    int b = a + rangeSize;
                    if (b > MAX_TOKEN)
                    {
                        b = a;
                        a = b - rangeSize;
                    }
                    return IntKey.range(a, b);
                };
            case NO_OVERLP:
                int delta = TOKEN_RANGE_SIZE / samples;
                return new Gen<>()
                {
                    int idx = 0;

                    @Override
                    public Range next(RandomSource ignore)
                    {
                        int a = delta * idx++;
                        int b = a + delta;
                        return IntKey.range(a, b);
                    }
                };
            default:
                throw new AssertionError();
        }
    }

    private static String stats(LongArrayList list, boolean isTime)
    {
        LongUnaryOperator fn = isTime ? TimeUnit.NANOSECONDS::toMicros : l -> l;
        String postfix = isTime ? "micro" : "";
        long[] array = list.toLongArray();
        Arrays.sort(array);
        StringBuilder sb = new StringBuilder();
        sb.append("Min: ").append(fn.applyAsLong(array[0])).append(postfix);
        sb.append(", Median: ").append(fn.applyAsLong(array[array.length / 2])).append(postfix);
        sb.append(", Max: ").append(fn.applyAsLong(array[array.length - 1])).append(postfix);
        return sb.toString();
    }

    private static <T> T timed(LongArrayList target, Supplier<T> fn)
    {
        long nowNs = System.nanoTime();
        try
        {
            return fn.get();
        }
        finally
        {
            target.add(System.nanoTime() - nowNs);
        }
    }

    private static List<Map.Entry<Range, Integer>> sort(List<Map.Entry<Range, Integer>> array)
    {
        array.sort((a, b) -> {
            int rc = a.getKey().compare(b.getKey());
            if (rc == 0)
                rc = a.getValue().compareTo(b.getValue());
            return rc;
        });
        return array;
    }


    private static RTree<Routing, Range, Integer> create(boolean useList)
    {
        if (useList)
            return new RTree<>(COMPARATOR, END_INCLUSIVE);
        return new RTree<>(COMPARATOR, ALL_INCLUSIVE);
    }

    private interface Model
    {
        Object actual();

        void put(Range range, int value);

        List<Map.Entry<Range, Integer>> intersectsToken(Routing key);

        List<Map.Entry<Range, Integer>> intersects(Range range);

        void done();
    }

    private static Model createModel(boolean useList)
    {
        return useList ? new ListModel() : new IntervalTreeModel();
    }

    private static class ListModel implements Model
    {
        List<Map.Entry<Range, Integer>> actual = new ArrayList<>();

        @Override
        public List<Map.Entry<Range, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
            actual.add(Map.entry(range, value));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            return actual.stream()
                         .filter(p -> p.getKey().contains(key))
                         .collect(Collectors.toList());
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            return actual.stream()
                         .filter(p -> p.getKey().compareIntersecting(range) == 0)
                         .collect(Collectors.toList());
        }

        @Override
        public void done()
        {

        }
    }

    private static class IntervalTreeModel implements Model
    {
        IntervalTree.Builder<Routing, Integer, Interval<Routing, Integer>> builder = IntervalTree.builder();
        IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual = null;

        @Override
        public IntervalTree<Routing, Integer, Interval<Routing, Integer>> actual()
        {
            return actual;
        }

        @Override
        public void put(Range range, int value)
        {
//            actual = actual.unbuild().add(new Interval<>((Routing) range.start(), (Routing) range.end(), value)).build();
            builder.add(new Interval<>((Routing) range.start(), (Routing) range.end(), value));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersectsToken(Routing key)
        {
            return map(actual.matches(key));
        }

        @Override
        public List<Map.Entry<Range, Integer>> intersects(Range range)
        {
            return map(actual.matches(new Interval<>((Routing) range.start(), (Routing) range.end(), null)));
        }

        private static List<Map.Entry<Range, Integer>> map(List<Interval<Routing, Integer>> matches)
        {
            return matches.stream().map(i -> Map.entry(IntKey.range(i.min, i.max), i.data)).collect(Collectors.toList());
        }

        @Override
        public void done()
        {
            assert builder != null;
            actual = builder.build();
            builder = null;
        }
    }
}