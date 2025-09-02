"""
Utilities for convenient
exporting the MPDS data
"""

import os
import random
import ujson as json
import polars as pl
from typing import Union


class MPDSExport(object):
    export_dir = "/tmp/_MPDS"

    human_names = {
        "length": "Bond lengths, A",
        "occurrence": "Counts",
        "bandgap": "Band gap, eV",
    }

    @classmethod
    def _verify_export_dir(cls):
        if not os.path.exists(cls.export_dir):
            os.makedirs(cls.export_dir)

        if not os.access(cls.export_dir, os.W_OK):
            raise RuntimeError("%s is not writable!" % cls.export_dir)

    @classmethod
    def _gen_basename(cls):
        basename = []
        random.seed()
        for _ in range(12):
            basename.append(
                random.choice(
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                )
            )
        return "".join(basename)

    @classmethod
    def _get_title(cls, term: Union[str, int]):
        if isinstance(term, int):
            return str(term)
        return cls.human_names.get(term, term.capitalize())

    @classmethod
    def save_plot(cls, data, columns, plottype, fmt="json", **kwargs):
        """
        Exports the data in the following formats for plotting:

        csv: for any external plotting application
        json: for a web-app at https://mpds.io/visavis
        """
        cls._verify_export_dir()
        plot = {"use_visavis_type": plottype, "payload": {}}

        if not isinstance(data, pl.DataFrame):
            raise TypeError("The 'data' parameter must be a Polars DataFrame")

        # сheck that columns are valid
        if not all(col in data.columns for col in columns):
            raise ValueError("Some specified columns are not in the DataFrame")

        if fmt == "csv":
            # export to CSV
            fmt_export = os.path.join(cls.export_dir, cls._gen_basename() + ".csv")
            with open(fmt_export, "w") as f_export:
                f_export.write(",".join(columns) + "\n")
                for row in data.select(columns).iter_rows():
                    f_export.write(",".join(map(str, row)) + "\n")

        elif fmt == "json":
            # export to JSON
            fmt_export = os.path.join(cls.export_dir, cls._gen_basename() + ".json")
            with open(fmt_export, "w") as f_export:
                if plottype == "bar":
                    # bar plot payload
                    plot["payload"] = {
                        "x": [data[columns[0]].to_list()],
                        "y": data[columns[1]].to_list(),
                        "xtitle": cls._get_title(columns[0]),
                        "ytitle": cls._get_title(columns[1]),
                    }

                elif plottype == "plot3d":
                    # 3D plot payload
                    plot["payload"] = {
                        "points": {"x": [], "y": [], "z": [], "labels": []},
                        "meshes": [],
                        "xtitle": cls._get_title(columns[0]),
                        "ytitle": cls._get_title(columns[1]),
                        "ztitle": cls._get_title(columns[2]),
                    }
                    recent_mesh = None
                    for row in data.iter_rows():
                        plot["payload"]["points"]["x"].append(
                            row[data.columns.index(columns[0])]
                        )
                        plot["payload"]["points"]["y"].append(
                            row[data.columns.index(columns[1])]
                        )
                        plot["payload"]["points"]["z"].append(
                            row[data.columns.index(columns[2])]
                        )
                        plot["payload"]["points"]["labels"].append(
                            row[data.columns.index(columns[3])]
                        )

                        if row[data.columns.index(columns[4])] != recent_mesh:
                            plot["payload"]["meshes"].append(
                                {"x": [], "y": [], "z": []}
                            )
                        recent_mesh = row[data.columns.index(columns[4])]

                        if plot["payload"]["meshes"]:
                            plot["payload"]["meshes"][-1]["x"].append(
                                row[data.columns.index(columns[0])]
                            )
                            plot["payload"]["meshes"][-1]["y"].append(
                                row[data.columns.index(columns[1])]
                            )
                            plot["payload"]["meshes"][-1]["z"].append(
                                row[data.columns.index(columns[2])]
                            )
                else:
                    raise RuntimeError(f"Error: {plottype} is an unknown plot type")

                if kwargs:
                    plot["payload"].update(kwargs)

                # write JSON to file
                f_export.write(json.dumps(plot, escape_forward_slashes=False, indent=4))

        else:
            raise ValueError(f"Unsupported format: {fmt}")

        return fmt_export

    @classmethod
    def save_df(cls, frame, tag):
        cls._verify_export_dir()
        if not isinstance(frame, pl.DataFrame):
            raise TypeError("Input frame must be a Polars DataFrame")

        if tag is None:
            tag = "-"

        pkl_export = os.path.join(
            cls.export_dir, "df" + str(tag) + "_" + cls._gen_basename() + ".pkl"
        )
        frame.write_parquet(pkl_export)  # cos pickle is not supported in polars
        return pkl_export

    @classmethod
    def save_model(cls, skmodel, tag):
        import _pickle as cPickle

        cls._verify_export_dir()
        if tag is None:
            tag = "-"

        pkl_export = os.path.join(
            cls.export_dir, "ml" + str(tag) + "_" + cls._gen_basename() + ".pkl"
        )
        with open(pkl_export, "wb") as f:
            cPickle.dump(skmodel, f)
        return pkl_export
