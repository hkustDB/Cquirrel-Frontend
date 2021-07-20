# gui-codegen
The GUI and CodeGen for Cquirrel.


## Environment setup
1. install AJU.jar into your mvn reposity.
  `mvn install:install-file -Dfile=./AJU.jar -DgroupId=org.hkust -DartifactId=AJU -Dversion=1.0-SNAPSHOT -Dpackaging=jar`
2. compile the `codegen.jar` file in codegen part. 
  `cd codegen`
  `mvn package -DskipTests -f .`  
4. according to the `README` file in GUI part, install the relevant components to boot the frontend and backend. 
