"""
Utilities for convenient
exporting the MPDS data
"""
import os
import random
import cPickle

import ujson as json

import pandas as pd


class MPDSExport(object):

    export_dir = "/tmp/_MPDS"

    human_names = {
        'length': 'Bond lengths, A',
        'occurrence': 'Counts',
        'bandgap': 'Band gap, eV'
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
            basename.append(random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
        return "".join(basename)

    @classmethod
    def _get_title(cls, term):
        return cls.human_names.get(term, term.capitalize())

    @classmethod
    def save_plot(cls, data, columns, plottype, fmt='json', **kwargs):
        """
        Exports the data in the following formats for plotting:

        csv: for any external plotting application
        json: for a web-app at https://mpds.io/visavis
        """
        cls._verify_export_dir()
        plot = {"use_visavis_type": plottype, "payload": {}}

        if isinstance(data, pd.DataFrame):
            iter_data = data.iterrows
            pointers = columns
        else:
            iter_data = lambda: enumerate(data)
            pointers = range(len(data[0]))

        if fmt == 'csv':
            fmt_export = os.path.join(cls.export_dir, cls._gen_basename() + ".csv")
            f_export = open(fmt_export, "w")
            f_export.write("%s\n" % ",".join(map(str, columns)))
            for _, row in iter_data():
                f_export.write("%s\n" % ",".join([str(row[i]) for i in pointers]))
            f_export.close()

        else:
            fmt_export = os.path.join(cls.export_dir, cls._gen_basename() + ".json")
            f_export = open(fmt_export, "w")

            if plottype == 'bar':

                plot["payload"] = {"x": [], "y": [], "xtitle": cls._get_title(columns[0]), "ytitle": cls._get_title(columns[1])}

                for _, row in iter_data():
                    plot["payload"]["x"].append(row[pointers[0]])
                    plot["payload"]["y"].append(row[pointers[1]])

            elif plottype == 'plot3d':

                plot["payload"]["points"] = {"x": [], "y": [], "z": [], "labels": []}
                plot["payload"]["meshes"] = []
                plot["payload"]["xtitle"] = cls._get_title(columns[0])
                plot["payload"]["ytitle"] = cls._get_title(columns[1])
                plot["payload"]["ztitle"] = cls._get_title(columns[2])
                recent_mesh = 0

                for _, row in iter_data():
                    plot["payload"]["points"]["x"].append(row[pointers[0]])
                    plot["payload"]["points"]["y"].append(row[pointers[1]])
                    plot["payload"]["points"]["z"].append(row[pointers[2]])
                    plot["payload"]["points"]["labels"].append(row[pointers[3]])

                    if row[4] != recent_mesh:
                        plot["payload"]["meshes"].append({"x": [], "y": [], "z": []})
                    recent_mesh = row[4]

                    if plot["payload"]["meshes"]:
                        plot["payload"]["meshes"][-1]["x"].append(row[pointers[0]])
                        plot["payload"]["meshes"][-1]["y"].append(row[pointers[1]])
                        plot["payload"]["meshes"][-1]["z"].append(row[pointers[2]])

            if kwargs:
                plot["payload"].update(kwargs)

            else: raise RuntimeError("\r\nError: %s is an unknown plot type" % plottype)

            f_export.write(json.dumps(plot, escape_forward_slashes=False, indent=4))
            f_export.close()

        return fmt_export

    @classmethod
    def save_df(cls, frame, tag):
        cls._verify_export_dir()
        if tag is None:
            tag = '-'

        pkl_export = os.path.join(cls.export_dir, 'df' + str(tag) + '_' + cls._gen_basename() + ".pkl")
        frame.to_pickle(pkl_export)
        return pkl_export

    @classmethod
    def save_model(cls, skmodel, tag):
        cls._verify_export_dir()
        if tag is None:
            tag = '-'

        pkl_export = os.path.join(cls.export_dir, 'ml' + str(tag) + '_' + cls._gen_basename() + ".pkl")
        with open(pkl_export, 'wb') as f:
            cPickle.dump(skmodel, f, cPickle.HIGHEST_PROTOCOL)
        return pkl_export
