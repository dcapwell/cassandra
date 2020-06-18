package org.apache.cassandra.config;

public interface Converter<Original, Current>
{
    Class<Original> getInputType();

    Current apply(Original value);

    public static final class IdentityConverter implements Converter<Object, Object>
    {
        public Class<Object> getInputType()
        {
            return null; // null means 'unchanged'  mostly used for renames
        }

        public Object apply(Object value)
        {
            return value;
        }
    }

    public static final class MillisDurationConverter implements Converter<Long, Duration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public Duration apply(Long value)
        {
            if (value == null)
                return null;
            return Duration.inMilliseconds(value.longValue());
        }
    }
}
