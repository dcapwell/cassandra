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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.CheckForNull;

import com.google.common.collect.AbstractIterator;

public class RTree<Token, Range, Value> implements Iterable<Map.Entry<Range, Value>>
{
    public interface Accessor<Token, Range>
    {
        Token start(Range range);
        Token end(Range range);
        boolean contains(Range range, Token token);
        boolean intersects(Range range, Token start, Token end);
        default boolean intersects(Range left, Range right)
        {
            return intersects(left, start(right), end(right));
        }
    }

    private final Comparator<Token> comparator;
    private final Accessor<Token, Range> accessor;
    private final int sizeTarget;
    private final int numChildren;
    private Node node = new Node();

    public RTree(Comparator<Token> comparator, Accessor<Token, Range> accessor)
    {
        this(comparator, accessor, 1 << 7, 6);
    }

    public RTree(Comparator<Token> comparator, Accessor<Token, Range> accessor, int sizeTarget, int numChildren)
    {
        this.comparator = comparator;
        this.accessor = accessor;
        this.sizeTarget = sizeTarget;
        this.numChildren = numChildren;
    }

    public List<Value> get(Range range)
    {
        List<Value> matches = new ArrayList<>();
        get(range, e -> matches.add(e.getValue()));
        return matches;
    }

    public boolean get(Range range, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        class Matcher implements Consumer<Map.Entry<Range, Value>>
        {
            boolean match = false;

            @Override
            public void accept(Map.Entry<Range, Value> e)
            {
                match = true;
                onMatch.accept(e);
            }
        }
        Matcher matcher =  new Matcher();
        node.search(range, matcher, e -> e.getKey().equals(range), Function.identity());
        return matcher.match;
    }

    public List<Map.Entry<Range, Value>> search(Range range)
    {
        List<Map.Entry<Range, Value>> matches = new ArrayList<>();
        search(range, matches::add);
        return matches;
    }

    public void search(Range range, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        node.search(range, onMatch, ignore -> true, Function.identity());
    }

    public List<Value> find(Range range)
    {
        List<Value> matches = new ArrayList<>();
        find(range, matches::add);
        return matches;
    }

    public void find(Range range, Consumer<Value> onMatch)
    {
        node.search(range, onMatch, ignore -> true, Map.Entry::getValue);
    }

    public List<Map.Entry<Range, Value>> searchToken(Token token)
    {
        List<Map.Entry<Range, Value>> matches = new ArrayList<>();
        searchToken(token, matches::add);
        return matches;
    }

    public void searchToken(Token token, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        node.searchToken(token, onMatch, ignore -> true, Function.identity());
    }

    public List<Value> findToken(Token token)
    {
        List<Value> matches = new ArrayList<>();
        findToken(token, matches::add);
        return matches;
    }

    public void findToken(Token token, Consumer<Value> onMatch)
    {
        node.searchToken(token, onMatch, ignore -> true, Map.Entry::getValue);
    }

    public boolean add(Range key, Value value)
    {
        node.add(key, value);
        return true;
    }

    public int remove(Range key)
    {
        return node.removeIf(e -> e.getKey().equals(key));
    }

    public int remove(Range key, Value value)
    {
        var match = Map.entry(key, value);
        return node.removeIf(match::equals);
    }
    
    public void clear()
    {
        node = new Node();
    }

    public int size()
    {
        return node.size;
    }

    public boolean isEmpty()
    {
        return node.size == 0;
    }

    public String displayTree()
    {
        StringBuilder sb = new StringBuilder();
        node.displayTree(0, sb);
        return sb.toString();
    }

    @Override
    public Iterator<Map.Entry<Range, Value>> iterator()
    {
        return node.iterator();
    }

    public Stream<Map.Entry<Range, Value>> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    private class Node implements Iterable<Map.Entry<Range, Value>>
    {
        private List<Map.Entry<Range, Value>> values = new ArrayList<>(10);
        private List<Node> children = null;
        private int size = 0;
        private Token minStart, maxStart, minEnd, maxEnd;

        int removeIf(Predicate<Map.Entry<Range, Value>> condition)
        {
            if (minStart == null)
                return 0;
            if (children != null)
            {
                int sum = 0;
                for (Node node : children)
                    sum += node.removeIf(condition);
                size -= sum;
                return sum;
            }
            class Counter {int value;}
            Counter counter = new Counter();
            values.removeIf(e -> {
                if (condition.test(e))
                {
                    counter.value++;
                    return true;
                }
                return false;
            });
            size -= counter.value;
            if (values.isEmpty())
                minStart = maxStart = minEnd = maxEnd = null;
            return counter.value;
        }

        void add(Range range, Value value)
        {
            size++;
            if (minStart == null)
            {
                minStart = maxStart = accessor.start(range);
                minEnd = maxEnd = accessor.end(range);
            }
            else
            {
                Token start = accessor.start(range);
                minStart = min(minStart, start);
                maxStart = max(maxStart, start);
                Token end = accessor.end(range);
                minEnd = min(minEnd, end);
                maxEnd = max(maxEnd, end);
            }
            if (children != null)
            {
                findBestMatch(range).add(range, value);
                return;
            }
            values.add(new MutableEntry(range, value));
            if (shouldSplit())
                split();
        }

        private Node findBestMatch(Range range)
        {
            int topIdx = 0;
            Node node = children.get(0);
            int topScore = node.score(range);
            int size = node.size;
            for (int i = 1; i < children.size(); i++)
            {
                node = children.get(i);
                int score = node.score(range);
                if (score > topScore || (score == topScore && size > node.size))
                {
                    topIdx = i;
                    size = node.size;
                }
            }
            return children.get(topIdx);
        }

        private int score(Range range)
        {
            if (minStart == null)
                return 0;
            if (!intersects(range))
                return -10;
            int score = 5; // overlapps
            if (values != null) // is leaf
                score += 5;

            int startScore = 0;
            if (comparator.compare(maxStart, accessor.start(range)) <= 0)
                startScore += 10;
            else if (comparator.compare(minStart, accessor.start(range)) <= 0)
                startScore += 5;

            int endScore = 0;
            if (comparator.compare(minEnd, accessor.end(range)) >= 0)
                endScore += 10;
            else if (comparator.compare(maxEnd, accessor.end(range)) >= 0)
                endScore += 5;
            // if fully contained, then add the scores: 10 for largest bounds, 20 for smallest bounds
            if (!(startScore == 0 || endScore == 0))
                score += startScore + endScore;
            return score;
        }

        boolean shouldSplit()
        {
            return values.size() > sizeTarget
                   // if the same range is used over and over again, splitting doesn't do much
                   && !(comparator.compare(minStart, maxStart) == 0
                        && comparator.compare(minEnd, maxEnd) == 0);
        }

        void split()
        {
            List<Token> allEndpoints = new ArrayList<>(values.size() * 2);
            for (Map.Entry<Range, Value> a : values)
            {
                allEndpoints.add(accessor.start(a.getKey()));
                allEndpoints.add(accessor.end(a.getKey()));
            }
            allEndpoints.sort(comparator);
            List<Token> maxToken = new ArrayList<>(numChildren);
            int tick = allEndpoints.size() / numChildren;
            int offset = tick;
            for (int i = 0; i < numChildren; i++)
            {
                maxToken.add(allEndpoints.get(offset));
                offset += tick;
                if (offset >= allEndpoints.size())
                {
                    maxToken.add(allEndpoints.get(allEndpoints.size() - 1));
                    break;
                }
            }

            children = new ArrayList<>(numChildren);
            for (int i = 0; i < numChildren; i++)
                children.add(new Node());

            for (Map.Entry<Range, Value> a : values)
            {
                Token end = accessor.end(a.getKey());
                Node selected = null;
                for (int i = 0; i < numChildren; i++)
                {
                    if (comparator.compare(end, maxToken.get(i)) < 0)
                    {
                        selected = children.get(i);
                        break;
                    }
                }
                if (selected == null)
                    selected = children.get(children.size() - 1);
                selected.add(a.getKey(), a.getValue());
            }
            values.clear();
            values = null;
        }

        <T> void search(Range range, Consumer<T> matches, Predicate<Map.Entry<Range, Value>> predicate, Function<Map.Entry<Range, Value>, T> transformer)
        {
            if (minStart == null)
                return;
            if (!intersects(range))
                return;
            if (children != null)
            {
                children.forEach(n -> n.search(range, matches, predicate, transformer));
                return;
            }
            values.forEach(e -> {
                if (accessor.intersects(e.getKey(), range) && predicate.test(e))
                    matches.accept(transformer.apply(e));
            });
        }

        <T> void searchToken(Token token, Consumer<T> matches, Predicate<Map.Entry<Range, Value>> predicate, Function<Map.Entry<Range, Value>, T> transformer)
        {
            if (minStart == null)
                return;
            if (comparator.compare(minStart, token) >= 0)
                return;
            if (comparator.compare(maxEnd, token) < 0)
                return;
            if (children != null)
            {
                for (int i = 0, size = children.size(); i < size; i++)
                    children.get(i).searchToken(token, matches, predicate, transformer);
                return;
            }
            values.forEach(e -> {
                if (accessor.contains(e.getKey(), token) && predicate.test(e))
                    matches.accept(transformer.apply(e));
            });
        }

        boolean intersects(Range range)
        {
            return accessor.intersects(range, minStart, maxEnd);
        }

        private void displayTree(int level, StringBuilder sb)
        {
            for (int i = 0; i < level; i++)
                sb.append('\t');
            sb.append("start:(").append(minStart).append(", ").append(maxStart).append("), end:(").append(minEnd).append(", ").append(maxEnd).append("):");
            if (children != null)
            {
                sb.append('\n');
                children.forEach(n -> n.displayTree(level + 1, sb));
            }
            else
            {
                sb.append(' ').append(size).append('\n');
            }
        }

        @Override
        public String toString()
        {
            return "Node{" +
                   "minStart=" + minStart +
                   ", maxStart=" + maxStart +
                   ", minEnd=" + minEnd +
                   ", maxEnd=" + maxEnd +
                   ", values=" + values +
                   ", children=" + children +
                   '}';
        }

        private Token min(Token a, Token b)
        {
            return comparator.compare(a, b) < 0 ? a : b;
        }

        private Token max(Token a, Token b)
        {
            return comparator.compare(a, b) < 0 ? b : a;
        }

        @Override
        public Iterator<Map.Entry<Range, Value>> iterator()
        {
            if (values != null)
                return values.iterator();
            return new AbstractIterator<>()
            {
                private int index = 0;
                private Iterator<Map.Entry<Range, Value>> it = null;
                @CheckForNull
                @Override
                protected Map.Entry<Range, Value> computeNext()
                {
                    while (true)
                    {
                        if (it == null)
                        {
                            if (index == children.size())
                                return endOfData();
                            it = children.get(index++).iterator();
                        }
                        if (it.hasNext())
                            return it.next();
                        it = null;
                    }
                }
            };
        }
    }

    private static class MutableEntry<K, V> implements Map.Entry<K, V>
    {
        private final K k;
        private V v;

        private MutableEntry(K k, V v)
        {
            this.k = k;
            this.v = v;
        }

        @Override
        public K getKey()
        {
            return k;
        }

        @Override
        public V getValue()
        {
            return v;
        }

        @Override
        public V setValue(V value)
        {
            V previous = v;
            v = Objects.requireNonNull(value);
            return previous;
        }
    }
}