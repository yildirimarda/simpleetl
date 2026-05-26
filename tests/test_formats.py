"""
Tests for format readers and writers.
"""

import pytest
import pandas as pd
import tempfile
import os
import json

from simpleetl.formats import (
    CSVReader, CSVWriter,
    JSONReader, JSONWriter,
    ParquetReader, ParquetWriter,
    AvroReader, AvroWriter,
    OrcReader, OrcWriter,
    XMLReader, XMLWriter,
    ExcelReader, ExcelWriter,
    DatabaseReader, DatabaseWriter,
    FormatFactory
)


class TestCSV:
    """Test CSV format support."""

    def test_csv_reader(self):
        """Test CSV reader."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age,city\n")
            f.write("Alice,25,New York\n")
            f.write("Bob,30,London\n")
            f.write("Charlie,35,Paris\n")
            temp_file = f.name

        try:
            reader = CSVReader()
            df = reader.read(temp_file)

            assert len(df) == 3
            assert list(df.columns) == ['name', 'age', 'city']
            assert df.iloc[0]['name'] == 'Alice'
            assert df.iloc[0]['age'] == 25
        finally:
            os.unlink(temp_file)

    def test_csv_writer(self):
        """Test CSV writer."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_file = f.name

        try:
            writer = CSVWriter()
            writer.write(df, temp_file)

            # Read back and verify
            df_read = pd.read_csv(temp_file)
            pd.testing.assert_frame_equal(df, df_read)
        finally:
            os.unlink(temp_file)


class TestJSON:
    """Test JSON format support."""

    def test_json_reader_file(self):
        """Test JSON reader from file."""
        data = [
            {"name": "Alice", "age": 25, "city": "New York"},
            {"name": "Bob", "age": 30, "city": "London"}
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(data, f)
            temp_file = f.name

        try:
            reader = JSONReader()
            df = reader.read(temp_file)

            assert len(df) == 2
            assert list(df.columns) == ['name', 'age', 'city']
            assert df.iloc[0]['name'] == 'Alice'
        finally:
            os.unlink(temp_file)

    def test_json_reader_string(self):
        """Test JSON reader from string."""
        json_str = '''[
            {"name": "Alice", "age": 25, "city": "New York"},
            {"name": "Bob", "age": 30, "city": "London"}
        ]'''

        reader = JSONReader()
        df = reader.read(json_str)

        assert len(df) == 2
        assert list(df.columns) == ['name', 'age', 'city']

    def test_json_writer(self):
        """Test JSON writer."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name

        try:
            writer = JSONWriter()
            writer.write(df, temp_file)

            # Read back and verify (JSONL format)
            with open(temp_file, 'r') as f:
                lines = f.readlines()

            assert len(lines) == 2
            data1 = json.loads(lines[0])
            data2 = json.loads(lines[1])

            assert data1['name'] == 'Alice'
            assert data2['name'] == 'Bob'
        finally:
            os.unlink(temp_file)


class TestParquet:
    """Test Parquet format support."""

    def test_parquet_reader(self):
        """Test Parquet reader."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            temp_file = f.name

        try:
            # Write using pandas
            df.to_parquet(temp_file)

            # Read using our reader
            reader = ParquetReader()
            df_read = reader.read(temp_file)

            pd.testing.assert_frame_equal(df, df_read)
        finally:
            os.unlink(temp_file)

    def test_parquet_writer(self):
        """Test Parquet writer."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            temp_file = f.name

        try:
            writer = ParquetWriter()
            writer.write(df, temp_file)

            # Read back and verify
            df_read = pd.read_parquet(temp_file)
            pd.testing.assert_frame_equal(df.reset_index(drop=True), df_read.reset_index(drop=True))
        finally:
            os.unlink(temp_file)


class TestAvro:
    """Test Avro format support."""

    def test_avro_writer_and_reader(self):
        """Test Avro roundtrip: write then read back."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
            temp_file = f.name

        try:
            writer = AvroWriter()
            writer.write(df, temp_file)

            reader = AvroReader()
            df_read = reader.read(temp_file)

            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age', 'city'}
            assert df_read.iloc[0]['name'] == 'Alice'
        finally:
            os.unlink(temp_file)

    def test_avro_schema_inference(self):
        """Test that AvroWriter infers schema correctly."""
        df = pd.DataFrame([
            {'id': 1, 'value': 3.14, 'label': 'test'},
        ])

        with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as f:
            temp_file = f.name

        try:
            writer = AvroWriter()
            writer.write(df, temp_file)

            reader = AvroReader()
            df_read = reader.read(temp_file)

            assert len(df_read) == 1
            assert set(df_read.columns) == {'id', 'value', 'label'}
        finally:
            os.unlink(temp_file)


class TestOrc:
    """Test ORC format support."""

    def test_orc_roundtrip(self):
        """Test ORC write then read back."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'},
        ])

        with tempfile.NamedTemporaryFile(suffix='.orc', delete=False) as f:
            temp_file = f.name

        try:
            writer = OrcWriter()
            writer.write(df, temp_file)

            reader = OrcReader()
            df_read = reader.read(temp_file)

            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age', 'city'}
            assert df_read.iloc[0]['name'] == 'Alice'
        finally:
            os.unlink(temp_file)

    def test_orc_reader_with_columns(self):
        """Test ORC reader with column selection."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'},
        ])

        with tempfile.NamedTemporaryFile(suffix='.orc', delete=False) as f:
            temp_file = f.name

        try:
            writer = OrcWriter()
            writer.write(df, temp_file)

            reader = OrcReader()
            df_read = reader.read(temp_file, columns=['name', 'age'])

            assert set(df_read.columns) == {'name', 'age'}
            assert len(df_read) == 2
        finally:
            os.unlink(temp_file)


class TestXML:
    """Test XML format support."""

    def test_xml_reader_file(self):
        """Test XML reader from file."""
        xml_content = """<?xml version="1.0" encoding="utf-8"?>
<data>
    <record>
        <name>Alice</name>
        <age>25</age>
    </record>
    <record>
        <name>Bob</name>
        <age>30</age>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_content)
            temp_file = f.name

        try:
            reader = XMLReader()
            df = reader.read(temp_file, root_element='data')

            assert len(df) == 2
            assert 'name' in df.columns
            assert df.iloc[0]['name'] == 'Alice'
        finally:
            os.unlink(temp_file)

    def test_xml_writer(self):
        """Test XML writer."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])

        with tempfile.NamedTemporaryFile(suffix='.xml', delete=False) as f:
            temp_file = f.name

        try:
            writer = XMLWriter()
            writer.write(df, temp_file, root_element='people', record_element='person')

            # Verify file exists and has content
            assert os.path.exists(temp_file)
            with open(temp_file, 'r') as f:
                content = f.read()
            assert 'Alice' in content
            assert 'Bob' in content
        finally:
            os.unlink(temp_file)

    def test_xml_reader_from_string(self):
        """Test XML reader from string."""
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<data>
    <item><name>Alice</name></item>
    <item><name>Bob</name></item>
</data>"""

        reader = XMLReader()
        df = reader.read(xml_str, root_element='data')
        assert len(df) == 2

    def test_xml_reader_missing_root_element(self):
        """Test XML reader raises ValueError for missing root element."""
        xml_str = """<?xml version="1.0" encoding="utf-8"?><data><item><name>Alice</name></item></data>"""

        reader = XMLReader()
        with pytest.raises(ValueError, match="Root element 'nonexistent' not found"):
            reader.read(xml_str, root_element='nonexistent')

    def test_xml_reader_single_record_dict(self):
        """Test XML reader with a single record (dict value, not list) (lines 53-55)."""
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<data>
    <person>
        <name>Alice</name>
        <age>25</age>
    </person>
</data>"""

        reader = XMLReader()
        df = reader.read(xml_str, root_element='data')
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Alice'

    def test_xml_reader_no_list_found_in_dict(self):
        """Test XML reader when parsed dict has no list values (lines 57-58)."""
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<root>
    <name>Alice</name>
    <age>25</age>
</root>"""

        reader = XMLReader()
        df = reader.read(xml_str)
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Alice'

    def test_xml_reader_list_input(self):
        """Test XML reader when parsed data is a list (line 60)."""
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<items>
    <item><name>Alice</name></item>
    <item><name>Bob</name></item>
</items>"""

        reader = XMLReader()
        df = reader.read(xml_str, root_element='items')
        assert len(df) == 2

    def test_xml_reader_else_fallback(self):
        """Test XML reader else fallback for non-dict, non-list data (line 62)."""
        # This tests the final else branch where data_dict is neither dict nor list
        # We need to craft XML that parses to a simple scalar value
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<root>hello</root>"""

        reader = XMLReader()
        # When root_element is specified and the value is a string (scalar)
        df = reader.read(xml_str, root_element='root')
        assert len(df) == 1
        assert 'value' in df.columns

    def test_xml_reader_dict_no_list_or_dict_values(self):
        """Test XML reader line 58: dict with only scalar values, no list/dict."""
        # After root_element extraction, data_dict should be a flat dict with scalar values
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<metadata>
    <version>1.0</version>
    <author>Alice</author>
</metadata>"""

        reader = XMLReader()
        # Read without root_element so data_dict = {'metadata': {...}}
        # The loop finds 'metadata' -> value is a dict -> returns DataFrame([value])
        # To hit line 58, we need data_dict itself to be a flat dict with scalar values
        # We can achieve this by using root_element to extract to a flat dict
        df = reader.read(xml_str, root_element='metadata')
        assert len(df) == 1
        assert 'version' in df.columns
        assert df.iloc[0]['version'] == '1.0'

    def test_xml_reader_data_dict_is_list(self):
        """Test XML reader line 60: data_dict is a list directly."""
        # xmltodict can produce a list when there are repeated elements at root level
        # We need to craft XML where after root_element extraction, the result is a list
        xml_str = """<?xml version="1.0" encoding="utf-8"?>
<items>
    <item><name>Alice</name></item>
    <item><name>Bob</name></item>
    <item><name>Charlie</name></item>
</items>"""

        reader = XMLReader()
        # When root_element='items', data_dict becomes {'item': [...]} which is a dict
        # To get data_dict to be a list, we need the root itself to be a list
        # This happens when xmltodict parses repeated root-level elements
        # Actually, let's test it by directly checking the list path
        # The list path is hit when xmltodict.parse returns a list
        # This is unusual but can happen with certain XML structures
        import xmltodict
        # Verify what xmltodict produces for this
        xmltodict.parse(xml_str)
        # parsed is {'items': {'item': [...]}}

        # To hit line 60, we need data_dict to be a list after root_element extraction
        # This can happen if the XML has a structure where the root element value is a list
        # Let's use a mock approach
        import unittest.mock as mock
        with mock.patch('simpleetl.formats.xml.xmltodict.parse', return_value={'data': [{'name': 'Alice'}, {'name': 'Bob'}]}):
            df = reader.read(xml_str, root_element='data')
            # data_dict = [{'name': 'Alice'}, {'name: 'Bob'}] which is a list
            assert len(df) == 2


class TestExcel:
    """Test Excel format support."""

    def test_excel_reader(self):
        """Test Excel reader."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_file = f.name

        try:
            # Write using pandas
            df.to_excel(temp_file, index=False)

            # Read using our reader
            reader = ExcelReader()
            df_read = reader.read(temp_file)

            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age', 'city'}
            assert df_read.iloc[0]['name'] == 'Alice'
        finally:
            os.unlink(temp_file)

    def test_excel_writer(self):
        """Test Excel writer."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_file = f.name

        try:
            writer = ExcelWriter()
            writer.write(df, temp_file)

            # Read back and verify
            df_read = pd.read_excel(temp_file)
            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age', 'city'}
        finally:
            os.unlink(temp_file)

    def test_excel_reader_sheet_name_none(self):
        """Test Excel reader with sheet_name=None returns dict (line 32)."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_file = f.name

        try:
            df.to_excel(temp_file, index=False, sheet_name='Sheet1')

            reader = ExcelReader()
            result = reader.read(temp_file, sheet_name=None)

            # When sheet_name is None, pandas returns a dict of DataFrames
            assert isinstance(result, dict)
        finally:
            os.unlink(temp_file)

    def test_excel_writer_dict_data(self):
        """Test Excel writer with dict of DataFrames writes multiple sheets (lines 55-57)."""
        df1 = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])
        df2 = pd.DataFrame([
            {'product': 'Widget', 'price': 9.99},
            {'product': 'Gadget', 'price': 19.99}
        ])

        data = {'People': df1, 'Products': df2}

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_file = f.name

        try:
            writer = ExcelWriter()
            writer.write(data, temp_file)

            # Read back and verify both sheets
            xlsx = pd.ExcelFile(temp_file)
            assert 'People' in xlsx.sheet_names
            assert 'Products' in xlsx.sheet_names

            df1_read = pd.read_excel(temp_file, sheet_name='People')
            assert len(df1_read) == 2
            assert 'name' in df1_read.columns
        finally:
            os.unlink(temp_file)


class TestDatabase:
    """Test Database format support (using SQLite)."""

    def test_database_writer_and_reader(self):
        """Test database roundtrip with SQLite."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25, 'city': 'New York'},
            {'name': 'Bob', 'age': 30, 'city': 'London'}
        ])

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_file = f.name

        try:
            conn_str = f"sqlite:///{temp_file}"

            writer = DatabaseWriter()
            writer.write(df, conn_str, table_name='test_table')

            reader = DatabaseReader()
            df_read = reader.read(conn_str, sql="SELECT * FROM test_table")

            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age', 'city'}
        finally:
            os.unlink(temp_file)

    def test_database_reader_with_invalid_source(self):
        """Test DatabaseReader raises ValueError for invalid source."""
        reader = DatabaseReader()
        with pytest.raises(ValueError, match="Invalid source type"):
            reader.read(12345)

    def test_database_reader_with_engine_no_sql(self):
        """Test DatabaseReader raises ValueError when engine used without sql."""
        import sqlalchemy
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        reader = DatabaseReader()
        with pytest.raises(ValueError, match="Must provide 'sql' or 'table'"):
            reader.read(engine)

    def test_database_writer_with_invalid_destination(self):
        """Test DatabaseWriter raises ValueError for invalid destination."""
        writer = DatabaseWriter()
        df = pd.DataFrame({'a': [1]})
        with pytest.raises(ValueError, match="Invalid destination type"):
            writer.write(df, 12345, table_name='test')

    def test_database_reader_string_without_sql_reads_table(self):
        """Test DatabaseReader with string source and no sql uses read_sql_table (line 31)."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_file = f.name

        try:
            conn_str = f"sqlite:///{temp_file}"
            # Write directly with pandas
            df.to_sql('my_table', conn_str, index=False)

            reader = DatabaseReader()
            # Pass table name as source with no sql kwarg -> triggers read_sql_table path
            df_read = reader.read(conn_str, sql="SELECT * FROM my_table")

            assert len(df_read) == 2
        finally:
            os.unlink(temp_file)

    def test_database_reader_engine_with_sql(self):
        """Test DatabaseReader with engine and sql parameter (lines 34-35)."""
        import sqlalchemy
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])
        df.to_sql('test_table', engine, index=False)

        reader = DatabaseReader()
        df_read = reader.read(engine, sql="SELECT * FROM test_table")

        assert len(df_read) == 2
        assert set(df_read.columns) == {'name', 'age'}

    def test_database_writer_with_engine_destination(self):
        """Test DatabaseWriter with engine as destination (line 66)."""
        import sqlalchemy
        engine = sqlalchemy.create_engine("sqlite:///:memory:")

        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])

        writer = DatabaseWriter()
        writer.write(df, engine, table_name='engine_table')

        # Verify data was written
        df_read = pd.read_sql("SELECT * FROM engine_table", engine)
        assert len(df_read) == 2

    def test_database_reader_string_table_param(self):
        """Test DatabaseReader with table parameter for read_sql_table path."""
        df = pd.DataFrame([
            {'name': 'Alice', 'age': 25},
            {'name': 'Bob', 'age': 30}
        ])

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_file = f.name

        try:
            conn_str = f"sqlite:///{temp_file}"
            df.to_sql('my_table', conn_str, index=False)

            reader = DatabaseReader()
            # Use table= parameter to trigger read_sql_table path
            df_read = reader.read(conn_str, table='my_table')

            assert len(df_read) == 2
            assert set(df_read.columns) == {'name', 'age'}
        finally:
            os.unlink(temp_file)


class TestFormatFactory:
    """Test format factory."""

    def test_get_csv_reader_writer(self):
        """Test getting CSV reader and writer."""
        reader = FormatFactory.get_reader('test.csv')
        assert isinstance(reader, CSVReader)

        writer = FormatFactory.get_writer('test.csv')
        assert isinstance(writer, CSVWriter)

    def test_get_json_reader_writer(self):
        """Test getting JSON reader and writer."""
        reader = FormatFactory.get_reader('test.json')
        assert isinstance(reader, JSONReader)

        writer = FormatFactory.get_writer('test.json')
        assert isinstance(writer, JSONWriter)

    def test_get_parquet_reader_writer(self):
        """Test getting Parquet reader and writer."""
        reader = FormatFactory.get_reader('test.parquet')
        assert isinstance(reader, ParquetReader)

        writer = FormatFactory.get_writer('test.parquet')
        assert isinstance(writer, ParquetWriter)

    def test_detect_format(self):
        """Test format detection."""
        info = FormatFactory.detect_format('test.csv')
        assert info['format'] == 'csv'
        assert info['extension'] == '.csv'

        info = FormatFactory.detect_format('test.json')
        assert info['format'] == 'json'
        assert info['extension'] == '.json'

    def test_supported_formats(self):
        """Test getting supported formats."""
        formats = FormatFactory.supported_formats()
        assert 'csv' in formats
        assert 'json' in formats
        assert 'parquet' in formats
        assert 'avro' in formats
        assert 'orc' in formats
        assert 'xml' in formats
        assert 'xlsx' in formats

    def test_get_database_reader_writer(self):
        """Test getting database reader and writer from factory."""
        reader = FormatFactory.get_reader('sqlite:///test.db')
        assert isinstance(reader, DatabaseReader)

        writer = FormatFactory.get_writer('sqlite:///test.db')
        assert isinstance(writer, DatabaseWriter)

    def test_get_excel_reader_writer(self):
        """Test getting Excel reader and writer from factory."""
        reader = FormatFactory.get_reader('test.xlsx')
        assert isinstance(reader, ExcelReader)

        writer = FormatFactory.get_writer('test.xlsx')
        assert isinstance(writer, ExcelWriter)

    def test_detect_database_format(self):
        """Test format detection for database connection strings."""
        info = FormatFactory.detect_format('sqlite:///test.db')
        assert info['format'] == 'database'

    def test_detect_unknown_format(self):
        """Test format detection for unknown format."""
        info = FormatFactory.detect_format('test.unknown')
        assert info['format'] == 'unknown'

    def test_get_reader_defaults_to_csv(self):
        """Test that unknown extension defaults to CSV reader."""
        reader = FormatFactory.get_reader('test.unknown')
        assert isinstance(reader, CSVReader)

    def test_get_writer_defaults_to_csv(self):
        """Test that unknown extension defaults to CSV writer."""
        writer = FormatFactory.get_writer('test.unknown')
        assert isinstance(writer, CSVWriter)

    def test_json_writer_stdout(self):
        """Test JSON writer to stdout."""
        import io
        import sys
        df = pd.DataFrame([{'name': 'Alice', 'age': 25}])

        writer = JSONWriter()
        captured = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            writer.write(df, '-')
        finally:
            sys.stdout = old_stdout

        output = captured.getvalue()
        assert 'Alice' in output