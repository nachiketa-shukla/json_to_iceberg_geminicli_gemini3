import polars as pl
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from typing import Optional

class JsonToIcebergPipeline:
    def __init__(
        self,
        catalog_uri: str,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        catalog_name: str = "default",
        catalog_token: Optional[str] = None,
        s3_region: str = "us-east-1"
    ):
        """
        Initializes the pipeline with connection details to the Iceberg REST catalog
        and the underlying S3-compatible object store.
        """
        self.catalog_properties = {
            "type": "rest",
            "uri": catalog_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.region": s3_region,
        }
        if catalog_token:
            self.catalog_properties["token"] = catalog_token
            
        self.catalog = load_catalog(catalog_name, **self.catalog_properties)

    def _transform_null_types(self, arrow_table: pa.Table) -> pa.Table:
        """
        Traverses the PyArrow schema and recursively replaces all instances of 
        pa.null() with pa.string(). Iceberg does not support a generic NullType, 
        so we must provide a concrete type (String is safest for inferred JSON data).
        """
        def _replace_null_with_string(typ: pa.DataType) -> pa.DataType:
            if pa.types.is_null(typ):
                return pa.string()
            elif pa.types.is_list(typ):
                return pa.list_(_replace_null_with_string(typ.value_type))
            elif pa.types.is_large_list(typ):
                return pa.large_list(_replace_null_with_string(typ.value_type))
            elif pa.types.is_struct(typ):
                return pa.struct([
                    pa.field(f.name, _replace_null_with_string(f.type), f.nullable)
                    for f in typ
                ])
            return typ

        new_fields = [
            pa.field(field.name, _replace_null_with_string(field.type), field.nullable)
            for field in arrow_table.schema
        ]
        
        new_schema = pa.schema(new_fields)
        return arrow_table.cast(new_schema)

    def process_file(
        self, 
        json_path: str, 
        table_identifier: str, 
        is_ndjson: bool = False
    ) -> None:
        """
        Orchestrates reading the JSON file, transforming its schema,
        and appending it to the Iceberg table.
        """
        print(f"Reading JSON file: {json_path}")
        # Polars handles highly nested JSON structures effectively natively
        if is_ndjson:
            df = pl.read_ndjson(json_path)
        else:
            df = pl.read_json(json_path)
            
        print("Converting to PyArrow and transforming schema to remove Null types...")
        arrow_table = df.to_arrow()
        cleaned_table = self._transform_null_types(arrow_table)
        
        namespace = table_identifier.split(".")[0]
        
        # Ensure namespace exists
        try:
            self.catalog.load_namespace(namespace)
        except NoSuchNamespaceError:
            print(f"Namespace '{namespace}' not found. Creating it...")
            self.catalog.create_namespace(namespace)

        try:
            iceberg_table = self.catalog.load_table(table_identifier)
            print(f"Table '{table_identifier}' found.")
        except NoSuchTableError:
            print(f"Table '{table_identifier}' not found. Creating table using inferred schema...")
            # create_table uses the PyArrow schema and automatically converts it to an Iceberg schema
            iceberg_table = self.catalog.create_table(
                identifier=table_identifier,
                schema=cleaned_table.schema,
            )

        print(f"Appending {cleaned_table.num_rows} records to {table_identifier}...")
        # Appends data to the table. PyIceberg handles writing Parquet files to S3
        # and committing the new snapshot to the REST catalog.
        iceberg_table.append(cleaned_table)
        print("Successfully written to Iceberg.")
