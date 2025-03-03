import os
import zipfile
from abc import ABC, abstractmethod

import pandas as pd

# Defune data ingestion process
class IngestData(ABC):
    @abstractmethod
    def ingest(self, filepath: str) -> pd.DataFrame:
        pass

# Define Zipped filed ingestion process
class ZipIngestData(IngestData):
    def ingest(self, filepath: str) -> pd.DataFrame:
        
        if not filepath.endswith('.zip'):
            raise ValueError(f'Filepath is not a zip file: {filepath}')
        
        with zipfile.ZipFile(filepath, "r") as z:
            z.extractall("extracted_data")
        
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith('.csv')]

        if len(csv_files) == 0:
            raise ValueError(f'No CSV files found in the zip file: {filepath}')
        
        if len(csv_files) > 1:
            raise ValueError(f'Multiple CSV files found in the zip file: {filepath}')
        
        df = pd.read_csv(f'extracted_data/{csv_files[0]}')
        return df

class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(filepath: str) -> IngestData:
        if filepath.endswith('.zip'):
            return ZipIngestData()
        else:
            raise ValueError(f'Filepath is not a supported format: {filepath}')


# Example usage
if __name__ == '__main__':
    # filepath = '../data/archive.zip'
    filepath = "C:\\Users\\amrit\\AppData\\Local\\Temp\\pytest-of-amrit\\pytest-3\\test_zip_ingest_data_no_csv0\\test_no_csv.zip"
    ingestor = DataIngestorFactory.get_data_ingestor(filepath)
    df = ingestor.ingest(filepath)
    print(df.head())

