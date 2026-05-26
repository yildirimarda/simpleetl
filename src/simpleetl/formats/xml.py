"""
XML format reader and writer using xmltodict.
"""

import xmltodict
import pandas as pd
from .base import DataReader, DataWriter
from ..core.filesystem import is_cloud_path, get_filesystem


class XMLReader(DataReader):
    """Read data from XML files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from an XML file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the XML file or XML string.
            **kwargs: Additional arguments for XML parsing.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.
        """
        root_element = kwargs.pop('root_element', None)

        # Read XML content
        if source.strip().startswith('<'):
            xml_content = source
        elif is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'r', encoding='utf-8') as f:
                xml_content = f.read()
        else:
            with open(source, 'r', encoding='utf-8') as f:
                xml_content = f.read()

        data_dict = xmltodict.parse(xml_content, **kwargs)

        if root_element:
            if root_element in data_dict:
                data_dict = data_dict[root_element]
            else:
                raise ValueError(
                    f"Root element '{root_element}' not found in XML"
                )

        if isinstance(data_dict, dict):
            for key, value in data_dict.items():
                if isinstance(value, list):
                    return pd.DataFrame(value)
                elif isinstance(value, dict):
                    return pd.DataFrame([value])
            return pd.DataFrame([data_dict])
        elif isinstance(data_dict, list):
            return pd.DataFrame(data_dict)
        else:
            return pd.DataFrame([{'value': data_dict}])


class XMLWriter(DataWriter):
    """Write data to XML files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to an XML file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output XML file.
            **kwargs: Additional arguments for XML generation.
                Supports 'filesystem' for an fsspec filesystem instance.
        """
        root_element = kwargs.pop('root_element', 'data')
        record_element = kwargs.pop('record_element', 'record')
        filesystem = kwargs.pop('filesystem', None)

        records = data.to_dict('records')
        xml_data = {root_element: {record_element: records}}
        xml_str = xmltodict.unparse(xml_data, **kwargs)

        if is_cloud_path(destination):
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, 'w', encoding='utf-8') as f:
                f.write(xml_str)
        else:
            with open(destination, 'w', encoding='utf-8') as f:
                f.write(xml_str)