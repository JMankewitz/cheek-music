#SETUP - Libraries
library(tidyverse)
library(spotifyr)
library(here)

# READ DATA
## Rounds
rounds_csvs <- list.files(path = "raw_data", 
                          pattern = "rounds.csv$", 
                          recursive = T, 
                          full.names = T)
d_rounds <- do.call("rbind", 
                    sapply(rounds_csvs, read.csv, simplify = FALSE)) |> 
  tibble::rownames_to_column("source_file") |> 
  separate_wider_delim(source_file, delim = "/", 
                       names = c("file_source", 
                                 "league_game", 
                                 "file")) |> 
  select(-file_source, -file) |> 
  janitor::clean_names() |> 
  rename(round_id = id)
  
# Competitors
competitors_csvs <- list.files(path = "raw_data", 
                          pattern = "competitors.csv$", 
                          recursive = T, 
                          full.names = T)

d_competitors <- do.call("rbind", 
                    lapply(competitors_csvs, 
                           read.csv)) |> 
  janitor::clean_names() |> 
  distinct() |> 
  rename(competitor_id = id)

## Submissions

submissions_csvs <- list.files(path = "raw_data", 
                               pattern = "submissions.csv$", 
                               recursive = T, 
                               full.names = T)

d_submissions <- do.call("rbind", 
                         lapply(submissions_csvs, 
                                read.csv)) |> 
  janitor::clean_names() |> 
  distinct() |> 
  rename(submission_time = created)

## Votes

votes_csvs <- list.files(path = "raw_data", 
                               pattern = "votes.csv$", 
                               recursive = T, 
                               full.names = T)

d_votes <- do.call("rbind", 
                         lapply(votes_csvs, 
                                read.csv)) |> 
  janitor::clean_names() |> 
  distinct() |> 
  rename(vote_time = created)


# GENERATE SPOTIFY DATA

access_token <- get_spotify_access_token()

# get_track_audio_features has a max of 100 songs, 
# so need to split the list of songs into lists of 100
# and rbind

#NOTE: could get nasty once we have way more songs, but is tractable now

song_submissions <- str_remove(d_submissions$spotify_uri, "spotify:track:")

song_splits <- split(song_submissions, ceiling(seq_along(song_submissions)/50))

d_track_info <- do.call("rbind", 
                        lapply(song_splits, 
                               spotifyr::get_tracks)) |> 
  unnest(cols = artists, names_sep = "_") |> 
  select(id, spotify_uri = uri, artists_name, 
         tack_name = name, explicit, popularity, 
         album_name = album.name, album_release_data = album.release_date) |> 
  group_by(across(c(-artists_name))) |> 
  summarise(artist = paste0(sort(unique(artists_name)), collapse = " AND "))

d_song_features <- do.call("rbind", 
                lapply(song_splits, 
                       spotifyr::get_track_audio_features)) |> 
  select(id, spotify_uri = uri, everything())

# WRITE TO PROCESSED FOLDER

write_path = "processed_data"

write_csv(d_competitors, paste0(here(write_path, "rounds.csv")))
write_csv(d_competitors, paste0(here(write_path, "competitors.csv")))
write_csv(d_competitors, paste0(here(write_path, "submissions.csv")))
write_csv(d_competitors, paste0(here(write_path, "votes.csv")))
write_csv(d_competitors, paste0(here(write_path, "song_features.csv")))
write_csv(d_competitors, paste0(here(write_path, "track_info.csv")))
