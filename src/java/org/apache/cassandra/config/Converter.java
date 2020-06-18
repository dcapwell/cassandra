package org.apache.cassandra.config;

public interface Converter
{
    Object apply(Object value);

    public static final class IdentityConverter implements Converter
    {
        public Object apply(Object value)
        {
            return value;
        }
    }

    public static abstract class DurationConverter implements Converter
    {
        private final String unit;

        protected DurationConverter(String unit)
        {
            this.unit = unit;
        }

        public Object apply(Object object)
        {
            assert object instanceof String : "only strings allowed but given " + object;
            String value = (String) object;
            return Duration.inMilliseconds(Long.parseLong(value));
        }
    }

    public static final class MillisDurationConverter extends DurationConverter
    {
        protected MillisDurationConverter()
        {
            super("ms");
        }
    }
}
