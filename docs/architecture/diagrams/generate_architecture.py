#!/usr/bin/env python3
"""
Generate Zomato Data Platform Architecture Diagram.

Requires: pip install diagrams
Usage:    python generate_architecture.py

Produces: zomato_data_platform_architecture.png
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.compute import EC2, EC2AutoScaling, ECS, EMR, Lambda
from diagrams.aws.database import Aurora, Dynamodb, RDS
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Spark, Flink, Trino
from diagrams.onprem.queue import Kafka
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
        # PIPELINE 1 - BATCH ETL
        # ================================================================
        with Cluster("Pipeline-1: Batch ETL"):
            sqoop = EMR("Apache Sqoop\n(Amazon EMR)")
            s3_raw_p1 = S3("S3 Raw\n(ORC)")

        # ================================================================
        # PIPELINE 2 - CDC
        # ================================================================
        with Cluster("Pipeline-2: CDC (Debezium)"):
            with Cluster("Kafka Connect\n(Distributed)"):
                worker_a = EC2("Worker-A")
                worker_b = EC2("Worker-B")
                worker_c = EC2("Worker-C")

        # ================================================================
        # SHARED KAFKA CLUSTER (P1 + P2)
        # ================================================================
        with Cluster("Self-Hosted Kafka Cluster 1\n(Amazon EC2)"):
            kafka1 = Kafka("Kafka Brokers")
            with Cluster("Topics"):
                topic_menu = Kafka("menu")
                topic_orders = Kafka("orders")
                topic_promo = Kafka("promo")
                topic_users = Kafka("users")

        # ================================================================
        # FLINK CEP (P1 + P2)
        # ================================================================
        with Cluster("Stream Processing (P1 + P2)"):
            flink_cep = Flink("Amazon Flink\n(CEP)")

        # Iceberg + ORC output
        with Cluster("Curated Layer (P1 + P2)"):
            s3_iceberg = S3("S3 Curated\n(ORC + Iceberg)")

        # ================================================================
        # PIPELINE 3 - DYNAMODB STREAMS
        # ================================================================
        with Cluster("Pipeline-3: DynamoDB Processing"):
            ecs_stream = ECS("ECS Multi-AZ\n(Stream to S3)")
            s3_json = S3("S3 Raw\n(JSON)")
            emr_spark = EMR("Apache Spark\n(Amazon EMR)")
            s3_curated_p3 = S3("S3 Curated\n(ORC)")

        # ================================================================
        # PIPELINE 4 - REAL-TIME
        # ================================================================
        with Cluster("Pipeline-4: Real-time Events"):
            producer = EC2("Custom\nProducer")

            with Cluster("Self-Hosted Kafka Cluster 2\n(Amazon EC2)"):
                kafka2 = Kafka("Kafka Brokers")

            flink_rt = Flink("Amazon Flink\n(Real-time)")
            s3_rt = S3("S3\n(ORC)")

        # ================================================================
        # DRUID PATH (P4)
        # ================================================================
        with Cluster("Real-time OLAP"):
            kafka3 = Kafka("Kafka Cluster 3\n(Intermediate)")
            ec2_as = EC2AutoScaling("EC2\nAuto-Scaling")
            druid = Druid("Apache Druid\n(OLAP)")

        # ================================================================
        # QUERY LAYER - TRINO
        # ================================================================
        with Cluster("Trino Query Engine\n(ECS · R8g Instances)"):
            trino_adhoc = Trino("Adhoc\nClusters")
            trino_etl = Trino("ETL\nClusters")
            trino_report = Trino("Reporting\nClusters")

        # ================================================================
        # CONNECTIONS
        # ================================================================

        # Pipeline 1: Aurora → Sqoop → S3 → Kafka → Flink
        aurora >> Edge(label="bulk import") >> sqoop
        sqoop >> Edge(label="ORC") >> s3_raw_p1
        s3_raw_p1 >> kafka1

        # Pipeline 2: Aurora → Debezium Workers → Kafka
        aurora >> Edge(label="binlog") >> worker_a
        aurora >> worker_b
        aurora >> worker_c
        worker_a >> Edge(label="Avro") >> kafka1
        worker_b >> kafka1
        worker_c >> kafka1

        # Kafka topics flow
        kafka1 - topic_menu
        kafka1 - topic_orders
        kafka1 - topic_promo
        kafka1 - topic_users

        # Kafka → Flink CEP
        kafka1 >> Edge(label="Avro") >> flink_cep

        # Flink feedback loop
        flink_cep >> Edge(label="feedback\nloop", style="dashed") >> kafka1

        # Flink → Iceberg → S3
        flink_cep >> Edge(label="Iceberg\n+ ORC") >> s3_iceberg

        # Pipeline 3: DynamoDB → ECS → S3 JSON → Spark → S3 ORC
        dynamodb >> Edge(label="DynamoDB\nStream") >> ecs_stream
        ecs_stream >> Edge(label="JSON") >> s3_json
        s3_json >> emr_spark
        emr_spark >> Edge(label="ORC") >> s3_curated_p3

        # Pipeline 4: Apps → Producer → Kafka → Flink
        microservices >> producer
        webapp >> producer
        mobile >> producer
        producer >> kafka2
        kafka2 >> flink_rt

        # Flink dual output
        flink_rt >> Edge(label="ORC") >> s3_rt
        flink_rt >> kafka3

        # Druid path
        kafka3 >> ec2_as
        ec2_as >> druid

        # All S3 → Trino
        s3_iceberg >> trino_adhoc
        s3_iceberg >> trino_etl
        s3_iceberg >> trino_report

        s3_curated_p3 >> trino_adhoc
        s3_curated_p3 >> trino_etl
        s3_curated_p3 >> trino_report

        s3_rt >> trino_adhoc
        s3_rt >> trino_etl
        s3_rt >> trino_report


if __name__ == "__main__":
    main()
