---
title: 'MPDS platform Python API client'
tags:
  - materials-science
  - materials-informatics
  - materials-platform
  - crystallography
  - crystal-structure
  - phase-diagram
  - data-science
  - Python
authors:
 - name: Evgeny Blokhin
   orcid: 0000-0002-5333-3947
   affiliation: "1, 2"
 - name: Martin Uhrin
   affiliation: 3
affiliations:
 - name: Tilde Materials Informatics, Berlin, Germany
   index: 1
 - name: Materials Platform for Data Science, Ltd., Tallinn, Estonia
   index: 2
 - name: Ecole polytechnique federale de Lausanne, Lausanne, Switzerland
   index: 3
date: 19 July 2018
bibliography: paper.bib
---

# Summary

The MPDS platform [@mpds] is an online database for inorganic chemistry and materials science with nearly two million entries: physical properties, crystalline structures, phase diagrams etc., available via API, ready for the modern data-intensive applications. The source of the data are nearly 300 thousands of peer-reviewed articles, books and conference proceedings, published since 1891 until today.

To work with the MPDS platform API, any programming language, able to execute HTTP requests and handle the JSON output, can be employed. However, one of the most frequently used languages for data processing is Python. Therefore we provide a client library for Python (versions 2.7 and 3.6). Installed as any other Python package via ``pip``, this client library takes care of many data retrieval aspects, such as querying, pagination, error handling, response validation, proper data extraction and more. Additionally, even more advanced transformations can be done with the aid of this library [@labs], such as calculating the crystalline descriptors for machine learning, sample ordering of the disordered alloy structures etc. The MPDS platform API client library was created as a part of the ``matminer`` package [@Ward:2018] for data mining in materials science (LBNL, Berkeley, USA) and then became a standalone package.

We encourage the MPDS platform users to adopt this library for building their own materials data infrastructures.

The source code for the ``MPDS platform Python API client`` has been archived to Zenodo with the linked DOI: [@zenodo]

# References
