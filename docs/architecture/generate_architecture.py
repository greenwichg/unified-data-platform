#!/usr/bin/env python3
"""
Generate Zomato Data Platform Architecture Diagram.

Requires: pip install diagrams
Usage:    python generate_architecture.py

Produces: zomato_data_platform_architecture.png
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, ManagedStreamingForKafka
from diagrams.aws.compute import EC2, EC2AutoScaling, ECS, EMR
from diagrams.aws.database import Aurora, Dynamodb
from diagrams.aws.integration import MQ
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Spark, Flink
from diagrams.onprem.database import Druid


def main():
    graph_attr = {
        "fontsize": "28",
        "fontname": "Helvetica Bold",
        "bgcolor": "white",
        "pad": "0.8",
        "nodesep": "0.8",
        "ranksep": "1.2",
        "splines": "ortho",
        "label": "Zomato's Data Platform Architecture on AWS",
        "labelloc": "t",
        "labeljust": "c",
    }

    node_attr = {
        "fontsize": "11",
        "fontname": "Helvetica",
    }

    with Diagram(
        "",
        filename="zomato_data_platform_architecture",
        show=False,
        direction="LR",
        graph_attr=graph_attr,
        node_attr=node_attr,
        outformat="png",
    ):

        # ================================================================
        # DATA SOURCES
        # ================================================================
        with Cluster("Data Sources"):

            with Cluster("Pipeline-1 & 2: Aurora MySQL"):
                aurora = Aurora("Aurora MySQL\n(Source DB)")

            with Cluster("Pipeline-3: DynamoDB"):
                dynamodb = Dynamodb("DynamoDB\n(Streams Enabled)")

            with Cluster("Pipeline-4: Applications"):
                microservices = ECS("Microservices")
                webapp = ECS("Web Application")
                mobile = ECS("Mobile Backend")

        # ================================================================
        # PIPELINE 1 - BATCH ETL (Spark JDBC)
        # ================================================================
        with Cluster("Pipeline-1: Batch ETL"):
            spark_jdbc = EMR("Spark JDBC\n(Amazon EMR)")
            s3_raw_p1 = S3("S3 Raw\n(Iceberg + ORC)")

        # ================================================================
        # PIPELINE 2 - CDC
        # ================================================================
        with Cluster("Pipeline-2: CDC (Debezium)"):
            with Cluster("Debezium on\nECS Fargate"):
                debezium = ECS("Debezium\nConnectors")

        # ================================================================
        # AMAZON MSK CLUSTER 1 (Primary - P2 + P4)
        # ================================================================
        with Cluster("Amazon MSK Cluster 1\n(Primary · IAM Auth)"):
            msk1 = ManagedStreamingForKafka("MSK Brokers\n(9x kafka.r8g.4xlarge)")
            with Cluster("Topics"):
                topic_menu = MQ("menu")
                topic_orders = MQ("orders")
                topic_promo = MQ("promo")
                topic_users = MQ("users")
                topic_generic = MQ("topics")

        # ================================================================
        # FLINK CDC (P2)
        # ================================================================
        with Cluster("Stream Processing (P2)"):
            flink_cdc = Flink("Amazon Managed\nFlink (CDC)")

        # Iceberg + ORC output
        with Cluster("Curated Layer (P1 + P2)"):
            s3_iceberg = S3("S3 Curated\n(ORC + Iceberg)")

        # ================================================================
        # PIPELINE 3 - DYNAMODB STREAMS
        # ================================================================
        with Cluster("Pipeline-3: DynamoDB Processing"):
            s3_json = S3("S3 Raw\n(JSON)")
            emr_spark = EMR("Apache Spark\n(Amazon EMR)")
            s3_curated_p3 = S3("S3 Curated\n(ORC)")

        # ================================================================
        # PIPELINE 4 - REAL-TIME
        # ================================================================
        with Cluster("Pipeline-4: Real-time Events"):
            producer = EC2("Custom\nProducer")
            flink_rt = Flink("Amazon Managed\nFlink (Real-time)")
            s3_rt = S3("S3\n(ORC)")

        # ================================================================
        # AMAZON MSK CLUSTER 2 (Secondary - Druid ingestion)
        # ================================================================
        with Cluster("Amazon MSK Cluster 2\n(Secondary · Druid Ingestion)"):
            msk2 = ManagedStreamingForKafka("MSK Brokers\n(6x kafka.r8g.2xlarge)")

        # ================================================================
        # DRUID PATH (P4)
        # ================================================================
        with Cluster("Real-time OLAP"):
            ec2_as = EC2AutoScaling("EC2 Consumer\nFleet (R8g)")
            druid = Druid("Apache Druid\n(R8g Instances)")

        # ================================================================
        # QUERY LAYER - ATHENA (serverless)
        # ================================================================
        with Cluster("Amazon Athena\n(Serverless · Trino-based)"):
            athena_adhoc = Athena("Adhoc\nWorkgroup")
            athena_etl = Athena("ETL\nWorkgroup")
            athena_report = Athena("Reporting\nWorkgroup")

        # ================================================================
        # SERVING LAYER
        # ================================================================
        with Cluster("Serving Layer\n(ECS Fargate)"):
            superset = ECS("Apache\nSuperset")
            redash = ECS("Redash")
            jupyter = ECS("JupyterHub")

        # ================================================================
        # CONNECTIONS
        # ================================================================

        # Pipeline 1: Aurora → Spark JDBC → S3 (Iceberg + ORC)
        aurora >> Edge(label="Spark JDBC") >> spark_jdbc
        spark_jdbc >> Edge(label="Iceberg\n+ ORC") >> s3_raw_p1
        s3_raw_p1 >> s3_iceberg

        # Pipeline 2: Aurora → Debezium (ECS) → MSK
        aurora >> Edge(label="binlog") >> debezium
        debezium >> Edge(label="Avro") >> msk1

        # MSK topics
        msk1 - topic_menu
        msk1 - topic_orders
        msk1 - topic_promo
        msk1 - topic_users
        msk1 - topic_generic

        # MSK Cluster 1 → Flink CDC
        msk1 >> Edge(label="Avro") >> flink_cdc

        # Flink CDC → Iceberg → S3
        flink_cdc >> Edge(label="Iceberg\n+ ORC") >> s3_iceberg

        # Pipeline 3: DynamoDB → Streams → S3 JSON → Spark → S3 ORC
        dynamodb >> Edge(label="DynamoDB\nStreams") >> s3_json
        s3_json >> emr_spark
        emr_spark >> Edge(label="ORC") >> s3_curated_p3

        # Pipeline 4: Apps → Producer → MSK Cluster 1 → Flink
        microservices >> producer
        webapp >> producer
        mobile >> producer
        producer >> msk1

        # MSK Cluster 1 → Flink Real-time (dual output)
        msk1 >> flink_rt

        # Flink dual output
        flink_rt >> Edge(label="ORC") >> s3_rt
        flink_rt >> Edge(label="druid-ingestion\n-events") >> msk2

        # MSK Cluster 2 → EC2 Consumer Fleet → Druid
        msk2 >> ec2_as
        ec2_as >> druid

        # All S3 → Athena
        s3_iceberg >> athena_adhoc
        s3_iceberg >> athena_etl
        s3_iceberg >> athena_report

        s3_curated_p3 >> athena_adhoc
        s3_curated_p3 >> athena_etl
        s3_curated_p3 >> athena_report

        s3_rt >> athena_adhoc
        s3_rt >> athena_etl
        s3_rt >> athena_report

        # Serving layer connections
        athena_adhoc >> superset
        athena_adhoc >> redash
        athena_adhoc >> jupyter
        druid >> superset
        druid >> redash


if __name__ == "__main__":
    main()
