package org.apache.cassandra.distributed.upgrade;

import org.apache.cassandra.distributed.shared.Versions;

public class MigrateDropColumns22To30To311Test extends MigrateDropColumns
{
    public MigrateDropColumns22To30To311Test()
    {
        super(Versions.Major.v22, Versions.Major.v30, Versions.Major.v3X);
    }
}
