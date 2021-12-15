## Scope

Duolingo is a company who's product is a language learning app. The app uses statistical techniques to optimize their user's speed and efficacy of learning languages.

In this project a data piepline is created for the use of the Duolingo researchers to help better understand their users behavior within the app.

Duolingo has made a data set available for public use, "learning_traces.csv", containing instances word views in the language they are learning. Each row of data contains the users ID, the language they are learning, the word, and various other statistics relevant to the users current session and history.

Duolingo has also made an accompanying data file available, "lexeme_reference.txt", which breaksdown lingustical attributes of the words used in "learning_traces.csv".

The final data file, "language-codes-full_json.json", contains the list ISO language codes, which are present in the "learning_traces.csv" data set.

This data pipeline downloads each of the three datasets mentioned above from S3, loads the data into separate Spark dataframes, cleans the data, reorganizes the data into a data model suited to aid in the analysis, writes the data to parquet files on S3 that can easily be loaded into Redshift for the analysts to run queries on.


## Data Modeling

A star schema was used, as pictured below:

![alt text](/imgs/schema_diagram.png "Star Schema")


#### Some questions that Duolingo researchers may ask are:

*What are the most common language pairs?*

*Which language pair has the most activity?*

*Are certain language pairs correlated with time-of-day?*

*Which language pair has the best retention?*

*Which language UI has the highest word retention across all learning languages?*