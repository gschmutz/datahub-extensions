import subprocess
import requests

# download jar
url = "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.0/avro-tools-1.11.0.jar"
r = requests.get(url, allow_redirects=True)
open('/tmp/avro-tools-1.11.0.jar', 'wb').write(r.content)

# Run a shell command
result = subprocess.run(["java", "-jar","/tmp/avro-tools-1.11.0.jar", "idl2schemata", "/Users/guido.schmutz/Downloads/datahub-restingest/src/main/avdl/Person.avdl", "person.avsc"], capture_output=True, text=True)

# Print the result
print("Return code:", result.returncode)
print("STDOUT:")
print(result.stdout)
print("STDERR:")
print(result.stderr)

