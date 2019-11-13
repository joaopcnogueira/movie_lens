library(tidyverse)

# Loading the first database ----
genomes <- read_csv("genome_scores.csv")
genomes %>% glimpse()

# Loading the second database ----
ratings <- read_csv("rating.csv")
ratings %>% glimpse()

# Joining the databases with dplyr ----
# Error: std::bad_alloc
genomes %>% 
    inner_join(ratings, by = "movieId")

# Using {data.table} ----
# Gives error to
library(data.table)

setDT(genomes)
setkey(genomes, "movieId")
key(genomes)

setDT(ratings)
setkey(ratings, "movieId")
key(ratings)


genomes[ratings, on="movieId", nomatch=0]


# Using Sparklyr ----
# please restart the session
library(tidyverse)
library(sparklyr)
sc <- spark_connect(master = "local")

genomes <- spark_read_csv(sc, name = "genome_scores.csv")
genomes %>% glimpse()

ratings <- spark_read_csv(sc, name = "rating.csv")
ratings %>% glimpse()

merged <- genomes %>% 
    inner_join(ratings, by = "movieId")

merged %>% head()



