#!/bin/bash
# Initialize Hive metastore schema in PostgreSQL.
# Runs schematool -initSchema; safe to run on repeated starts (skips if schema exists).

cat > /opt/hive/conf/hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://hive-metastore-postgresql/hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
</configuration>
EOF

/opt/hive/bin/schematool -dbType postgres -initSchema || echo "Schema already exists, skipping."
