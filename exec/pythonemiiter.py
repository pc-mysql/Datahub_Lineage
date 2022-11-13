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

def splitstring(param):
    param = param.lstrip('[')
    param = param.rstrip(']')
    list = param.split(' ')
    return list


def parseargs(args):        #解析python传过来的数组
    for i in range(1,len(args)):
        if i in [2,4]:
            args[i] = splitstring(args[i])

    return args

#
def emmit(readdataset,readtable,writerdataset,writertable,url):
    upstream_tables: List[UpstreamClass] = []
    for table in readtable:
        upstream_table = UpstreamClass(
            dataset=builder.make_dataset_urn(readdataset, table, "PROD"),
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
        upstream_tables.append(upstream_table)

    # Construct a lineage object.
    upstream_lineage = UpstreamLineage(upstreams=upstream_tables)

    # Construct a MetadataChangeProposalWrapper object.
    for table in writertable:
        lineage_mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=builder.make_dataset_urn(writerdataset,table),
            aspectName="upstreamLineage",
            aspect=upstream_lineage,
        )

    # Create an emitter to the GMS REST API.
    emitter = DatahubRestEmitter(url)

    # Emit metadata!
    emitter.emit_mcp(lineage_mcp)


print("the java transmitted parameters are",str(sys.argv))
params = parseargs(sys.argv)
print("the Converted parameters" + str(params))
params=params[1:]
emmit(*params)
















#
#
# upstream_table_1 = UpstreamClass(
#     dataset=builder.make_dataset_urn("bigquery", "upstream_table_1", "PROD"),
#     type=DatasetLineageTypeClass.TRANSFORMED,
# )
# upstream_tables: List[UpstreamClass] = [upstream_table_1]
# upstream_table_2 = UpstreamClass(
#     dataset=builder.make_dataset_urn("bigquery", "upstream_table_2", "PROD"),
#     type=DatasetLineageTypeClass.TRANSFORMED,
# )
# upstream_tables.append(upstream_table_2)
#
# # Construct a lineage object.
# upstream_lineage = UpstreamLineage(upstreams=upstream_tables)
#
# # Construct a MetadataChangeProposalWrapper object.
# lineage_mcp = MetadataChangeProposalWrapper(
#     entityType="dataset",
#     changeType=ChangeTypeClass.UPSERT,
#     entityUrn=builder.make_dataset_urn("bigquery", "downstream"),
#     aspectName="upstreamLineage",
#     aspect=upstream_lineage,
# )
#
# # Create an emitter to the GMS REST API.
# emitter = DatahubRestEmitter("http://localhost:8080")
#
# # Emit metadata!
# emitter.emit_mcp(lineage_mcp)