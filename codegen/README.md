# Code Generator
This component generates the code for a given json representation of a query and generates code corresponding to it. That code is then submitted to a flink cluster to run.

## Packages
The code is divided in to the following packages:
* checkerutils: this package is where generic sanity checks go
* codegenerator: this is where the actual generation happens.
Below is the structure:

![codegenerator_class_diagram](/readme_resources/codegenerator_class_diagram.png)
* jsonutils: This is where the parsing of the input json happens and returns a Node object.
* objects: These are the domain objects. Most of these objects are made as a result of parsing the input json file. The most important object is Node, an instance of it represents a query and it has these object: List<RelationProcessFunction> relationProcessFunctions, List<AggregateProcessFunction> aggregateProcessFunctions and Map<Relation, Relation> joinStructure.
Below is the class diagram for this package:

![objects_class_diagram](/readme_resources/objects_class_diagram.png)

* schema
This package describes the TPCH schema. The most important class is the RelationSchema class as it makes the actual relations used in the supported relations.

The annotations package has a collection of annotations for the developers to communicate with each other and is not functional, all annotations are for source files only.

The main class is the CodeGen class and is responsible for parsing the input arguments and passing them to the relevant classes.


## Usage
There is a total of 5 compulsory arguments, which are, in order:
1. The path to the json file
1. The path to put the generated code's jar
1. The path of the input to the generated code
1. The path of the output of the generated code
1. The I/O type of the generated code, there are 2 options: "file" and "kafka"

## Contributing
The master branch is protected and pushing to it is not possible. In order to contribute discuss with the team the new idea and make a corresponding jira story ticket with the proper description, then make a branch with the same jira id and raise a pull request and ask Qichen to review and merge it. Testing evidence must be provided for the branch to be merged.



