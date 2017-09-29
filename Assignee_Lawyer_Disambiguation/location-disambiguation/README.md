# Location Disambiguation

## Running the code

The program can be built and run using [maven](https://maven.apache.org/). Maven is not required, but it will handle downloading dependencies and setting the classpath, To run the program you need a configuration file with the following entries.

	mysql.host = <URL to ingest database server>
	mysql.user = <user name on ingest database>
	mysql.password = <password on ingest database>
	mysql.grant.database = <database containing the rawlocations table to disambiguate>
	
	location.database = <sqlite database containing geolocation data>
	location.path = <path to the geolocation database>
	location.raw_google.confidence_threshold = <threshold to use when matching to google database>
	location.match_threshold = <threshold to use for fuzzy string matching>
	
Then, assuming the configuration file is named `config.properties`, you can run the program by calling:

	mvn exec:java -Dexec.mainClass=lodi.App -Dexec.args=config.properties

	

## Cleaning Raw Locations

Raw locations are taken from the database. For each raw location, the following
transformations are applied to the city, state, and country fields:

* EOL characters are removed
* Pipe characters are removed
* Replacements in manual_replacement_library.txt are performed. These replacements try to
    replace the different ways that non-ASCII characters might be represented with the
    UNICODE equivalents.
* Replace any occurrences of patterns like {dot over (a)} with the character in
    parentheses.
* Remove XML/HTML tags
* Normalize the text to the canonical UNICODE form
* Remove patterns that appear in remove_patterns.txt
* For the country field, the replacements specified in country_patterns.txt are applied.
* If the cleaned country field is "US", then the replacements specified in
    state_abbreviations.txt are applied. These translate state names to their 2-letter
    codes.

## Geolocation database

The geolocation database contains 2 tables: cities, and google_cities. The cities table
contains all unique city-region-country combinations

### The Raw Google data

## Disambiguation

For each input location record, we create a fields by concatenating the cleaned values of
city, state, and country.
