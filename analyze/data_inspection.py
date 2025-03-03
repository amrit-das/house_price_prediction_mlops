from abc import ABC, abstractmethod
import pandas as pd

class DataInspectionStratergy(ABC):
    @abstractmethod
    def inspect(self, df: pd.DataFrame) -> None:
        """Inspect the data in the DataFrame

        Args:
            df (pd.DataFrame): DataFrame to inspect
        
        Returns:
            None
        """
        pass

class DataTypesInspectionStrategy(DataInspectionStratergy):
    def inspect(self, df: pd.DataFrame) -> None:
        """Inspect the data types of the DataFrame

        Args:
            df (pd.DataFrame): DataFrame to inspect
        
        Returns:
            None
        """
        print("Data Types:")
        print(df.dtypes)
        print("Schema Info:")
        print(df.info())