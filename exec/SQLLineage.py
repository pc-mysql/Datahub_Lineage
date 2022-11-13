from sqllineage.runner import LineageRunner

import logging
import sys
from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import ChangeTypeClass




sql = ""
sql = "insert into db1.table11 select * from db2.table21 union select * from db2.table22;"
sql += "insert into db3.table3 select * from db1.table11 join db1.table12;"
database = "test."



def parsesql(sql):
    result = LineageRunner(sql,verbose=True)
    source_tables = result.source_tables
    target_tables = result.target_tables
    source_tablename = []
    target_tablename = []

    for table in source_tables:
        source_tablename.append(database+str(table))

    for table in target_tables:
        target_tablename.append(database+str(table))

    return source_tablename,target_tablename

def submit_tables(dataset_name,source_table,target_table):
    upstream_tables: List[UpstreamClass] = []
    for table in source_table:
        upstream_table = UpstreamClass(
            dataset=builder.make_dataset_urn(dataset_name, table, "PROD"),
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
        upstream_tables.append(upstream_table)


    # Construct a lineage object.
    upstream_lineage = UpstreamLineage(upstreams=upstream_tables)


    # Construct a MetadataChangeProposalWrapper object.
    for table in target_table:
        lineage_mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=builder.make_dataset_urn(dataset_name, table),
            aspectName="upstreamLineage",
            aspect=upstream_lineage,
        )
        # Create an emitter to the GMS REST API.
        # Emit metadata!
        emitter.emit_mcp(lineage_mcp)


sys_argv = sys.argv
sql = sys_argv[1]
dataset_name = sys_argv[2]
url = sys_argv[3]
emitter = DatahubRestEmitter(url)
sqllist = sql.split(";")[:-1]
print(f"开始解析sql，一共有{len(sqllist)}条sql")
for i,sql in enumerate(sqllist):
    source_tablename,target_tablename = parsesql(sql)
    try:
        submit_tables(dataset_name,source_tablename,target_tablename)
        print(f"第{i}条sql执行成功")
    except:
        print(f"第{i}条sql执行失败")







