import requests
import json
import time

API_SEASONS_PATH = "https://api.pubg.com/shards/kakao/seasons/"

class ConfigManager:
    def __init__(self, config_filepath, metadata_filepath,
                players_id_path, players_data_path, random_players_list_path,
                overwrite_files, sample_player_ids_from_match, downloadTelemetry):
        self.api_key = ""
        with open(config_filepath) as config_file:
            self.api_key = json.load(config_file)["API_KEY"]
        if self.api_key == "":
            raise ValueError('API KEY MISSING in config file',config_filepath)

        self.request_headers = {
          "Authorization": self.api_key,
          "Accept": "application/vnd.api+json",
          "Accept-Encoding": "gzip"
        }
        self.metadata = self.updateAndRetrieveMetadata(metadata_filepath)

        self.players_id_path = players_id_path
        self.players_data_path = players_data_path
        self.random_players_list_path = random_players_list_path
        self.overwrite_files = overwrite_files
        self.sample_player_ids_from_match = sample_player_ids_from_match
        self.downloadTelemetry = downloadTelemetry


    def updateAndRetrieveMetadata(self, metadata_filepath):
        with open(metadata_filepath) as metadata_file:
            metadata = json.load(metadata_file)
            last_modified = metadata["seasons"]["lastModified"]
            # update season data every 3 days
            if (int(round(time.time() * 1000)) - last_modified > 259200000):
                response = requests.get(API_SEASONS_PATH, headers=self.request_headers)
                if response.ok:
                    metadata["seasons"] = json.loads(response.content)
                    metadata["seasons"]["lastModified"] = int(round(time.time() * 1000))
                    with open(metadata_filepath, "w") as metadata_outfile:
                        json.dump(metadata, metadata_outfile)
                        print("seasonal information updated")
            else:
                print("seasonal information up to date")
            return metadata
