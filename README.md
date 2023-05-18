# flink-cep-setups
Setting up apache flink to develop complex event pattern (CEP) system. 

The project consist of 6 maven projects written in Java and using the [apache-flink](https://flink.apache.org/) library.
Each project is designed to print an alert when it detect patterns on the custom event read from a file source.

The overview of all the projects follow similar pattern but with different implementation approaches.
- import apache-flink streaming and CEP libraries
- in the entry class, set up the execution environment for the flink application
- read event from a file source
- conversion to collection of a custom flink datastream objects
- optional setup of event timestamps and optional parallel processing to ensure ordering of records
- create a flink pattern to filter events that meets a specified threshold
- setup a process to monitor the pattern and generate an alert
- the alert is then printed to std out

Note that the some of the above steps are implemented somewhat differently from each other. However, idea is the same
This is possible by leveraging different APIs available from the apache-flink streaming and CEP libraries

# Useful Links
- [apache-flink](https://flink.apache.org/)
- [apache-flink-cep](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/libs/cep/)
- [CEP Intro](https://flink.apache.org/2016/04/06/introducing-complex-event-processing-cep-with-apache-flink/)


