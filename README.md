Ella Hanson

- Part 1 - 

The first mapper in the reduce-side join job, CountryCount, reads the Apache log files and emits the 
host as the key. The value includes "A" to signify that this is from the first file concat with "1" 
for the record count. The second mapper reads the hostname_country file and also emits the host
as the key. The value for this mapper includes a "B", signifying being from the second file, concat with 
the country that the host is from. 

All the emitted mapper values for a single host name is grouped for the
the reduccer to find the country from the lines including "B" and the count of that
hosts records from the line with "A". For each host the country is then emitted with the count of records
for that host. This data then used by the map-reduce job "CountrySum" to sum the countries count, where
the reducer reads in those seperate country counts and can find the total record sum for each country.

The last mapreduce job "CountrySort" emits the sum of records as the key and the country name
and the value. Then a WritableComparator reverses the order of the default sort. Then that the
reducer can then swap the key and value places of the country and count.

Command Lines
./gradlew run --args="CountryCount input_access_log input_hostname_country out_countrycount"
./gradlew run --args="CountrySum out_countrycount out_countrysum"
./gradlew run --args="CountrySort out_countrysum out_countrysort"



- Part 2 - 

The reduce-side join job URLCount joins the Apache log files and the and the hostname_country file on the
host name. One mapper emits the host as the key and the URL that the host visited. The other mapper emits
the host and the country that host is from. The reducer then iterates through all the emitted values for
the same host. Every occurance of a url is help from the first mapper and the country connected to the host,
which was emmited from the second mapper, is also recorded. The reducer then can emit a key of the url concat
with the country and a value of 1. A simple second MapReduce job, URLCount2, was used to sum the counts 
of the country/url pairs. The mapper emitted the same key and value as the reducer of the first job 
so that the reducer could group by key and find the sum for each of these pairs.

Then a final map reduce job sorted by country and then by count with the use of the composite key
CountryCountPair. This emitted a key of a CountryCountPair with the country and the count of the requests.
The value was emitted as the url. The shuffle phase then sorts first by country then by the countin descending
order. This leaves the reducer to place the data back in the correct position with the 
country and url as the key and the count as the value. 

Command Lines
./gradlew run --args="URLCount input_access_log input_hostname_country out_urlcount"
./gradlew run --args="URLCount2 out_urlcount out_urlcount2"
./gradlew run --args="URLCountSort out_urlcount2 out_urlcountsort"


- Part 3 - 

The first job is a reduce side join, Part3_1, which joins the two source files. The first mapper reads
the log file and emmits the host as the key and the url that was visited as the value.
The second mapper read the hostname file and also emmits the key as the host and the country where
that host is from as the value.
This joins the data on the host since the reducer recieves all the values that were emitted with the same host
key. The reducer then traverses through all the values, picking out all the urls which the hosts visited as well
as which country that host is from. Then for each of these urls the reducer emits the url as the key and the 
country which visited the url as the value. 

The second job uses the URLCountryPair composite key. The mapper emits the composite key with the URL and the country 
and with the value as the country. This way when the data is automatically sorted the composite key is sorted on url first then
by the country. This sets up the data so that when the data is grouped for the reducer by the url part of the
key. The reducer then interates through the sorted countries that visited the specific url, weeds out any duplicates,
and combines then into one string seperated by commas for the value. 

Command Lines
./gradlew run --args="Part3_1 input_access_log input_hostname_country out_part3_1"
./gradlew run --args="Part3_2 out_part3_1 out_part3_2"    
