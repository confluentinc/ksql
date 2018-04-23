# KSQL Documentation
The KSQL documentation is available on the Confluent Platform documentation site at [docs.confluent.io](https://docs.confluent.io/current/ksql/docs/index.html).

# Generating KSQL docs for Confluent Platform
The KSQL docs are imported from this public repo and built along with the rest of the Confluent Platform documentation. 

# Building the documentation locally

*This documentation is built using [Sphinx](http://sphinx-doc.org). It also uses some extensions for theming and REST API
 documentation support.
 
 Start by installing the requirements:
 
     pip install -r requirements.txt
 
 Then you can generate the HTML version of the docs:
 
     make html
 
 The root of the documentation will be at `_build/html/index.html`
 
 While editing the documentation, you can get a live preview using python-livepreview. Install the Python library:
 
     pip install livereload
 
 Then run the monitoring script in the background:
 
     python autoreload.py &
 
 If you install the [browser extensions](http://livereload.com/) then everything should update every time any files are
 saved without any manual steps on your part.

# Contributing
This documentation is built using [Sphinx](http://sphinx-doc.org). For information on how to contribute, see the [contributing guidelines](contributing.md).