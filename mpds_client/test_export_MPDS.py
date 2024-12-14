import unittest
import os
import polars as pl
from export_MPDS import MPDSExport 


class TestMPDSExport(unittest.TestCase):
    def test_save_plot_csv(self):
        """Test saving a plot in CSV format."""
        data = pl.DataFrame({
            "length": [1.2, 1.5, 1.8, 2.0, 2.2],
            "occurrence": [10, 15, 8, 20, 12]
        })
        columns = ["length", "occurrence"]
        plottype = "bar"

        exported_file = MPDSExport.save_plot(data, columns, plottype, fmt='csv')
        self.assertTrue(os.path.isfile(exported_file))
        self.assertTrue(exported_file.endswith(".csv"))

    def test_save_plot_json(self):
        """Test saving a plot in JSON format."""
        data = pl.DataFrame({
            "length": [1.2, 1.5, 1.8, 2.0, 2.2],
            "occurrence": [10, 15, 8, 20, 12]
        })
        columns = ["length", "occurrence"]
        plottype = "bar"

        exported_file = MPDSExport.save_plot(data, columns, plottype, fmt='json')
        self.assertTrue(os.path.isfile(exported_file))
        self.assertTrue(exported_file.endswith(".json"))

    def test_save_plot_3d_json(self):
        """Test saving a 3D plot in JSON format."""
        data = pl.DataFrame({
            "x": [1, 2, 3, 4],
            "y": [5, 6, 7, 8],
            "z": [9, 10, 11, 12],
            "labels": ["A", "B", "C", "D"],
            "meshes_id": [1, 1, 2, 2]
        })
        columns = ["x", "y", "z", "labels", "meshes_id"]
        plottype = "plot3d"

        exported_file = MPDSExport.save_plot(data, columns, plottype, fmt='json')
        self.assertTrue(os.path.isfile(exported_file))
        self.assertTrue(exported_file.endswith(".json"))

    def test_save_df(self):
        """Test saving Polars DataFrame."""
        data = pl.DataFrame({
            "column1": [1, 2, 3],
            "column2": [4, 5, 6]
        })
        tag = "test"

        exported_file = MPDSExport.save_df(data, tag)
        self.assertTrue(os.path.isfile(exported_file))
        self.assertTrue(exported_file.endswith(".parquet"))


if __name__ == "__main__":
    unittest.main()
