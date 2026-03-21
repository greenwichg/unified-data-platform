-- =============================================================================
-- Iceberg Table DDL: Menu Items
-- Catalog: iceberg | Schema: zomato
--
-- Stores menu item data from all restaurants. Updated via Pipeline 2 (CDC)
-- when restaurant owners modify their menus.
--
-- Partitioned by cuisine_type for efficient filtering in analytics queries.
-- Written in ORC format with Snappy compression.
-- =============================================================================

CREATE TABLE IF NOT EXISTS iceberg.zomato.menu_items (
    item_id                 VARCHAR     NOT NULL,
    restaurant_id           VARCHAR     NOT NULL,
    name                    VARCHAR     NOT NULL,
    description             VARCHAR,
    category                VARCHAR     NOT NULL  COMMENT 'e.g., Main Course, Starter, Dessert, Beverage',
    cuisine_type            VARCHAR     NOT NULL  COMMENT 'e.g., North Indian, South Indian, Chinese',
    price                   DECIMAL(10, 2) NOT NULL,
    discounted_price        DECIMAL(10, 2),
    is_vegetarian           BOOLEAN     NOT NULL,
    is_vegan                BOOLEAN     DEFAULT false,
    is_available            BOOLEAN     DEFAULT true,
    preparation_time_mins   INTEGER,
    rating                  REAL        COMMENT 'Average customer rating (0-5)',
    num_ratings             BIGINT      DEFAULT 0,
    total_orders            BIGINT      DEFAULT 0,
    image_url               VARCHAR,
    tags                    ARRAY(VARCHAR)  COMMENT 'e.g., bestseller, new, spicy',
    allergens               ARRAY(VARCHAR)  COMMENT 'e.g., nuts, dairy, gluten',
    nutritional_info        ROW(
                                calories    INTEGER,
                                protein_g   REAL,
                                carbs_g     REAL,
                                fat_g       REAL
                            ),
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6),
    cdc_operation           VARCHAR,
    processing_time         TIMESTAMP(6),
    dt                      DATE        NOT NULL COMMENT 'Partition column: last updated date'
)
WITH (
    format = 'ORC',
    format_version = 2,
    location = 's3://zomato-data-platform-raw-data-lake/iceberg/zomato/menu_items',
    partitioning = ARRAY['dt'],
    orc_compression = 'SNAPPY'
);
