# Simplified CQL grammar

## How to generate the grammar files

1. Install ANTLR 4.
   - On a Mac: `brew install antlr`.

2. Generate the Go files for the `SimplifiedCql` grammar:
    ```
    antlr -Dlanguage=Go antlr/SimplifiedCql.g4
    ```

Current ZDM code works with ANTLR 4.13.1, so if you have issues downloading it from system package managers you can:
1. Download JAR file from https://repo1.maven.org/maven2/org/antlr/antlr4/4.13.1
2. Generate Go files for simplified CQL grammar:
   ```
   java -Xmx500M -cp ".:/path/to/antlr4-4.13.1-complete.jar" org.antlr.v4.Tool -Dlanguage=Go antlr/SimplifiedCql.g4
   ```