import os
import requests
import json
import time
import scripts.fileUtils as fileUtils

API_PLAYERS_ENDPOINT = "https://api.pubg.com/shards/kakao/players?filter[playerNames]="
MATCH_API_REQUEST_ENDPOINT = "https://api.pubg.com/shards/kakao/matches/"

class DataCrawler:
    def __init__(self, config):
        self.config = config

    def downloadMatchTelemetryData(self, player_id, match_id, telemetry_URL):
        req_headers = self.config.request_headers
        overwrite = self.config.overwrite_files
        filepath = self.config.players_data_path+"telemetry_data/"+player_id+"_"+match_id+".json"

        if (not overwrite and os.path.isfile(filepath)):
            print("filename",filepath,"already exists. Skipping...")
            return

        response = None
        try:
            response = requests.get(telemetry_URL, headers=req_headers)
        except:
            print("ERROR: FAILED TO RETRIEVE TELEMETRY DATA. SKIPPING...")
            return

        print("response status:",response.status_code)
        if response.ok:
            with open(filepath, mode = 'w') as outfile:
                if (len(response.content) > 100):
                    json.dump(json.loads(response.content), outfile)
                    print("fetched telemetry match data")

    # retrieve all relavent match statistics for specified player
    def requestMatchStats(self, match_id, player_id):
        req_headers = self.config.request_headers
        samples_count = self.config.sample_player_ids_from_match
        samples_filepath = self.config.random_players_list_path

        match_response = None
        try:
            match_response = requests.get(MATCH_API_REQUEST_ENDPOINT+match_id, headers=req_headers)
        except:
            print("ERROR: FAILED TO RETRIEVE MATCH DATA. SKIPPING...")
            return {"responseStatus":401}
        print("fetched match data",match_id,"with response status:",match_response.status_code)

        # scrap in-game match stats from each match played
        if match_response.ok:
            match_dict = json.loads(match_response.content)
            if samples_count:
                fileUtils.extractRandomPlayerListFromMatch(samples_filepath, match_dict, samples_count)

            in_match_playerId = ""
            match_attributes = match_dict["data"]["attributes"]
            in_match_stats = None
            rank = 0
            won = None
            roster_id_list = []
            telemetry_URL = ""

            # find in-match stats of the target player
            for data in match_dict["included"]:
                if data["type"] == "asset":
                    telemetry_URL = data["attributes"]["URL"]

                if data["type"] == "participant":
                    if data["attributes"]["stats"]["playerId"] == player_id:
                        in_match_playerId = data["id"]
                        in_match_stats = data["attributes"]["stats"]

            # find other players in-match id in the same roster (as target player)
            for data in match_dict["included"]:
                if data["type"] == "roster":
                    for member in data["relationships"]["participants"]["data"]:
                        if member["id"] == in_match_playerId:
                            roster_id_list = [member["id"] for member in data["relationships"]["participants"]["data"]]
                            rank = data["attributes"]["stats"]["rank"]
                            won = data["attributes"]["won"]

            # find ign of players in the same roster (as target player)
            for data in match_dict["included"]:
                if data["type"] == "participant":
                    for i, roster_id in enumerate(roster_id_list):
                        if data["id"] == roster_id:
                            roster_id_list[i] = {"id":data["attributes"]["stats"]["playerId"],
                                                    "name":data["attributes"]["stats"]["name"]}

            return {"responseStatus": match_response.status_code,
                    "telemetryDataURL": telemetry_URL,
                    "matchId": match_id,
                    "matchAttributes": match_attributes,
                    "playerId": player_id,
                    "playerStats": in_match_stats,
                    "rank": rank,
                    "won": won,
                    "rosterIdList": roster_id_list}
        return {"responseStatus":match_response.status_code}

    def fetchAllMatchData(self, player_id, match_list):
        metadata = self.config.metadata
        req_headers = self.config.request_headers
        overwrite = self.config.overwrite_files
        filepath = self.config.players_data_path+"match_stats/"+player_id+".json"
        download_telemetry = self.config.downloadTelemetry

        if (len(match_list) ==0):
            return

        matchfile_abortflag = False
        if (not overwrite and os.path.isfile(filepath)):
            print("filename",filepath,"already exists. Skipping...")
            matchfile_abortflag = True
            if not download_telemetry:
                return

        # fetch all matches played in recent 14 days
        matchdata_lines = []
        for i, m in enumerate(match_list):
            match_data = self.requestMatchStats(m["id"], player_id)
            if (match_data["responseStatus"] != 200):
                continue
            if i<download_telemetry:
                self.downloadMatchTelemetryData(player_id, match_data["matchId"], match_data["telemetryDataURL"])
            elif matchfile_abortflag:
                return

            matchdata_lines.append(match_data)

        fileUtils.writeMatchDataToCsv(filepath, matchdata_lines)


    def fetchPlayerSeasonalStatsFromAPI(self, player_id):
        metadata = self.config.metadata
        req_headers = self.config.request_headers
        overwrite = self.config.overwrite_files
        filepath = self.config.players_data_path+"seasonal_stats/"+player_id+".json"

        if (not overwrite and os.path.isfile(filepath)):
            print("filename",filepath,"already exists. Skipping...")
            return

        curr_season_id = ""
        prev_season_id = ""
        for season in metadata["seasons"]["data"]:
            if season["attributes"]["isCurrentSeason"]:
                curr_season_id = season["id"]
        prev_season_id = metadata["seasons"]["data"][-1]["id"]

        req_url = "https://api.pubg.com/shards/kakao/players/"+player_id+"/seasons/"
        req_url2 = req_url + curr_season_id
        req_url = req_url + prev_season_id
        current_season = {}
        previous_season = {}
        combined_seasons_data = {}

        response = requests.get(req_url, headers=req_headers)
        print("response status:",response.status_code)
        while response.status_code == 429:
            time.sleep(6)
            response = requests.get(req_url, headers=req_headers)
            print("response status:",response.status_code)
        if response.ok:
            current_season = json.loads(response.content)

        response = requests.get(req_url2, headers=req_headers)
        print("response status:",response.status_code)
        while response.status_code == 429:
            time.sleep(6)
            response = requests.get(req_url2, headers=req_headers)
            print("response status:",response.status_code)
        if response.ok:
            previous_season = json.loads(response.content)

        combined_seasons_data = {"currentSeason": current_season,
                            "previousSeason": previous_season}
        with open(filepath, mode = 'w') as outfile:
            print("retrieved seasonal stats")
            json.dump(combined_seasons_data, outfile)


    def requestAndProcessPlayerDataFromAPI(self, url):
        req_headers = self.config.request_headers

        response = requests.get(url, headers=req_headers)
        print("response status:",response.status_code)
        if response.ok:
            players_dict = json.loads(response.content) # up to 6 players
            # get list of matches of each player
            for player_data in players_dict["data"]:
                print("retrieving",len(player_data["relationships"]["matches"]["data"]),"matches of", player_data["attributes"]["name"])
                self.fetchPlayerSeasonalStatsFromAPI(player_data["id"])
                self.fetchAllMatchData(player_data["id"], player_data["relationships"]["matches"]["data"])
        return response.status_code


    def run(self):
        id_path = self.config.players_id_path
        data_path = self.config.players_data_path
        fileUtils.autogenerateDataDirectories(data_path)

        players_count = sum([len(names) for names in [open(id_path+"/"+f).read().split() for f in os.listdir(id_path)]])
        print(players_count, "accounts found for data crawling...")
        progress = 0
        for filename in os.listdir(id_path):
            banned_players = open(id_path+"/"+filename).read().split()
            progress += len(banned_players)
            if filename[0] == '.':
                continue
            print("crawling data for ", len(banned_players),"accounts from file", filename)
            for i in range(0,len(banned_players),6):
                print("\nprogress:", progress+i,"/", players_count, "current:", filename)
                urlstring = ""
                for name in banned_players[i:min(i+6, len(banned_players))]:
                    urlstring += name+","
                urlstring = API_PLAYERS_ENDPOINT+urlstring[:-1]
                print(urlstring)
                while(self.requestAndProcessPlayerDataFromAPI(urlstring)==429):
                    time.sleep(6)
