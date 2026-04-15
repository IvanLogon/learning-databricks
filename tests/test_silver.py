# ============================================================================
# Data Quality Test Suite - Using pytest
# ============================================================================

import pytest
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs, sum as spark_sum
from io import StringIO

# ============================================================================
# Test Class for Data Quality
# ============================================================================
class TestSilverDataQuality:
    """Data quality tests for silver layer sales data"""
    
    @pytest.fixture(scope="class")
    def df(self):
        """Fixture to provide the DataFrame to all tests"""
        # Get or create SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Read from the silver layer table
        return spark.read.table("sales.silver.retail_store_sales")
    
    # ========================================================================
    # Test 1: Schema Validation
    # ========================================================================
    def test_schema_data_types(self, df):
        """Verify column data types are correct"""
        schema_dict = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        errors = []

        # Check item_id is string
        if schema_dict['item_id'] not in ['string']:
            errors.append(f"item_id has wrong type: {schema_dict['item_id']}")

        # Check quantity is int
        if schema_dict['quantity'] not in ['int', 'bigint']:
            errors.append(f"quantity should be int, got: {schema_dict['quantity']}")

        # Check price_per_unit is numeric
        if schema_dict['price_per_unit'] not in ['double', 'float', 'decimal']:
            errors.append(f"price_per_unit should be numeric, got: {schema_dict['price_per_unit']}")

        # Check total_spent is numeric
        if schema_dict['total_spent'] not in ['double', 'float', 'decimal']:
            errors.append(f"total_spent should be numeric, got: {schema_dict['total_spent']}")

        assert not errors, "Schema data type errors:\n" + "\n".join(errors)
    
    # ========================================================================
    # Test 2: Null Value Checks
    # ========================================================================
    def test_no_null_values_in_columns(self, df):
        """Verify no null values in any column after imputation"""
        null_counts = df.select(
            [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
             for c in df.columns]
        ).collect()[0].asDict()
        
        columns_with_nulls = {col_name: count for col_name, count in null_counts.items() if count > 0}
        
        assert len(columns_with_nulls) == 0, \
            f"Found null values in columns: {columns_with_nulls}"
    
    # ========================================================================
    # Test 3: Value Validation
    # ========================================================================
    def test_quantity_is_positive(self, df):
        """Verify all quantities are greater than 0"""
        invalid_count = df.filter(col('quantity') <= 0).count()
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with quantity <= 0"
    
    def test_price_per_unit_is_positive(self, df):
        """Verify all prices are greater than 0"""
        invalid_count = df.filter(col('price_per_unit') <= 0).count()
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with price_per_unit <= 0"
    
    def test_total_spent_is_positive(self, df):
        """Verify all totals are greater than 0"""
        invalid_count = df.filter(col('total_spent') <= 0).count()
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with total_spent <= 0"
    
    # ========================================================================
    # Test 4: Business Logic Validation
    # ========================================================================
    def test_total_spent_calculation_is_correct(self, df):
        """Validate total_spent = price_per_unit * quantity (within tolerance)"""
        tolerance = 0.01  # 1 cent tolerance for rounding
        
        validation_df = df.withColumn(
            'calculated_total',
            col('price_per_unit') * col('quantity')
        ).withColumn(
            'difference',
            spark_abs(col('total_spent') - col('calculated_total'))
        )
        
        invalid_count = validation_df.filter(col('difference') > tolerance).count()
        
        assert invalid_count == 0, \
            f"Found {invalid_count} rows where total_spent != price_per_unit × quantity (tolerance: ${tolerance})"
    
    # ========================================================================
    # Test 5: Duplicate Detection
    # ========================================================================
    def test_no_duplicate_records(self, df):
        """Check for unexpected duplicate records"""
        total_rows = df.count()
        distinct_rows = df.distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        assert duplicate_count == 0, \
            f"Found {duplicate_count} duplicate rows ({(duplicate_count/total_rows)*100:.2f}%)"
    
    # ========================================================================
    # Test 6: Data Completeness
    # ========================================================================
    def test_dataframe_is_not_empty(self, df):
        """Verify the DataFrame contains data"""
        row_count = df.count()
        assert row_count > 0, "DataFrame is empty - no data to write to silver layer"
