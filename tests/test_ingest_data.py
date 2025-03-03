import os
import pytest
import pandas as pd
import zipfile
from src.ingest_data import ZipIngestData, DataIngestorFactory

@pytest.fixture
def zip_file(tmp_path):
    # Create a temporary zip file with a CSV inside
    csv_content = "col1,col2\n1,2\n3,4"
    zip_path = tmp_path / "test.zip"
    csv_path = tmp_path / "test.csv"
    
    with open(csv_path, "w") as f:
        f.write(csv_content)
    
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname="test.csv")
    
    return zip_path

def test_zip_ingest_data(zip_file):
    ingestor = ZipIngestData()
    df = ingestor.ingest(str(zip_file))
    
    expected_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    pd.testing.assert_frame_equal(df, expected_df)

def test_zip_ingest_data_multiple_csv(tmp_path):
    # Create a temporary zip file with multiple CSVs inside
    zip_path = tmp_path / "test_multiple_csv.zip"
    
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("test1.csv", "col1,col2\n1,2")
        z.writestr("test2.csv", "col1,col2\n3,4")
    
    ingestor = ZipIngestData()
    
    with pytest.raises(ValueError, match="Multiple CSV files found in the zip file"):
        ingestor.ingest(str(zip_path))

def test_data_ingestor_factory(zip_file):
    ingestor = DataIngestorFactory.get_data_ingestor(str(zip_file))
    assert isinstance(ingestor, ZipIngestData)

def test_data_ingestor_factory_invalid_format():
    with pytest.raises(ValueError, match="Filepath is not a supported format"):
        DataIngestorFactory.get_data_ingestor("test.txt")