import requests
import json
import csv
import time
import os

API_KEY = ""
with open("config.json") as config_file:
    API_KEY = json.load(config_file)["API_KEY"]

PLAYER_API_REQUEST_ENDPOINT = "https://api.pubg.com/shards/kakao/players?filter[playerNames]="
MATCH_API_REQUEST_ENDPOINT = "https://api.pubg.com/shards/kakao/matches/"
PLAYERS_ID_PATH = "./player_ban_list"
PLAYERS_DATA_PATH = "./banned_players_data/"

header = {
  "Authorization": API_KEY,
  "Accept": "application/vnd.api+json",
  "Accept-Encoding": "gzip"
}
metadata = {}

# retrieve all relavent match statistics for specified player
def requestMatchStats(match_id, player_id, headers, match_url = "https://api.pubg.com/shards/kakao/matches/"):
    match_response = requests.get(match_url+match_id, headers=headers)
    #print("fetched match data",match_id,"with response status:",match_response.status_code)

    # scrap in-game match stats from each match played
    if match_response.ok:
        match_dict = json.loads(match_response.content)

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
                "matchId": match_id,
                "matchAttributes": match_attributes,
                "playerId": player_id,
                "playerStats": in_match_stats,
                "rank": rank,
                "won": won,
                "rosterIdList": roster_id_list,
                "telemetryDataURL": telemetry_URL}
    return {"responseStatus":match_response.status_code}

def downloadAndWriteMatchTelemetryData(filename, telemetry_URL, headers, overwrite = False):
    if (not overwrite and os.path.isfile(filename)):
        print("filename",filename,"already exists. Skipping...")
        return

    response = None
    try:
        response = requests.get(telemetry_URL, headers=headers)
    except:
        print("ERROR: FAILED TO RETRIEVE TELEMETRY DATA. SKIPPING...")
        return

    #print("response status:",response.status_code)
    if response.ok:
        with open(filename, mode = 'w') as outfile:
            if (len(response.content) > 100):
                json.dump(json.loads(response.content), outfile)
                print("fetched telemetry match data")

def retrieveAndWriteAllMatchData(filename, player_id, match_list, headers, overwrite = False, downloadTelemetry = False, telemetryLimit = 3):
    if (len(match_list) ==0):
        return

    abortFlag = False
    if (not overwrite and os.path.isfile(filename)):
        print("filename",filename,"already exists. Skipping...")
        abortFlag = True
        if not downloadTelemetry:
            return

    with open(filename, mode='w') as csv_file:
        # fetch all matches played in recent 14 days

        fieldnames = ["matchId", "createdAt", "gameMode", "mapName", "isCustomMatch", "duration", "rank", "won",
                        "DBNOs", "assists", "boosts", "damageDealt", "deathType", "headshotKills", "heals",
                        "killPlace", "killPoints", "killPointsDelta", "killStreaks", "kills", "lastKillPoints", "lastWinPoints",
                        "longestKill", "mostDamage", "rankPoints", "revives", "rideDistance", "roadKills", "swimDistance", "teamKills",
                        "timeSurvived", "vehicleDestroys", "walkDistance", "weaponsAcquired", "winPlace", "winPoints", "winPointsDelta",
                        "teammates", "telemetryDataURL"]

        writer = None
        if overwrite or not os.path.isfile(filename):
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

        for i, m in enumerate(match_list):
            match_data = requestMatchStats(m["id"], player_id, headers=headers, match_url=MATCH_API_REQUEST_ENDPOINT)

            if downloadTelemetry and i<telemetryLimit:
                downloadAndWriteMatchTelemetryData(PLAYERS_DATA_PATH+"telemetry/"+player_id+"_"+match_data["matchId"]+".json",
                                                    match_data["telemetryDataURL"], headers = headers)
            elif abortFlag:
                return

            roster_str = ""
            for teammate in match_data["rosterIdList"]:
                roster_str+=teammate["id"]+"&"
            roster_str = roster_str[:-1]

            match_data = {**match_data, **match_data["matchAttributes"], **match_data["playerStats"]}
            match_data["teammates"] = roster_str
            for unused_key in ["titleId", "shardId", "playerId", "stats",
                                "rosterIdList", "won", "tags", "matchAttributes","seasonState",
                                "name", "responseStatus", "playerStats"]:
                match_data.pop(unused_key)

            if writer != None:
                writer.writerow(match_data)

def retrieveAndWritePlayerSeasonalStatsFromAPI(filename, player_id, metadata, headers, overwrite = False):
    if (not overwrite and os.path.isfile(filename)):
        print("filename",filename,"already exists. Skipping...")
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
    two_seasons_data = {}

    response = requests.get(req_url, headers=headers)
    #print("response status:",response.status_code)
    while response.status_code == 429:
        time.sleep(6)
        response = requests.get(req_url, headers=headers)
        #print("response status:",response.status_code)
    if response.ok:
        current_season = json.loads(response.content)

    response = requests.get(req_url2, headers=headers)
    #print("response status:",response.status_code)
    while response.status_code == 429:
        time.sleep(6)
        response = requests.get(req_url2, headers=headers)
        #print("response status:",response.status_code)
    if response.ok:
        previous_season = json.loads(response.content)

    two_seasons_data = {"currentSeason": current_season,
                        "previousSeason": previous_season}
    with open(filename, mode = 'w') as outfile:
        print("retrieved seasonal stats")
        json.dump(two_seasons_data, outfile)

def requestAndProcessPlayerDataFromAPI(url, headers):
    global metadata
    response = requests.get(url, headers=headers)
    #print("response status:",response.status_code)
    if response.ok:
        players_dict = json.loads(response.content) # up to 6 players
        # get list of matches of each player
        for player_data in players_dict["data"]:
            print("retrieving",len(player_data["relationships"]["matches"]["data"]),"matches of", player_data["attributes"]["name"])
            retrieveAndWritePlayerSeasonalStatsFromAPI(PLAYERS_DATA_PATH+"seasonal_stats/"+player_data["id"]+".json",
                                                        player_data["id"], metadata, headers)
            retrieveAndWriteAllMatchData(PLAYERS_DATA_PATH+"match_stats/"+player_data["id"]+".csv",
                                        player_data["id"],
                                        player_data["relationships"]["matches"]["data"],
                                        headers = headers,
                                        overwrite = False,
                                        downloadTelemetry = True,
                                        telemetryLimit = 3)
    return response.status_code


def retrieveUpdatedMetadata():
    with open("metadata.json") as metadata_file:
        metadata = json.load(metadata_file)
        last_modified = metadata["seasons"]["lastModified"]
        # update season data every 3 days
        if (int(round(time.time() * 1000)) - last_modified > 259200000):
            response = requests.get("https://api.pubg.com/shards/kakao/seasons/", headers=header)
            if response.ok:
                metadata["seasons"] = json.loads(response.content)
                metadata["seasons"]["lastModified"] = int(round(time.time() * 1000))
                with open("metadata.json", "w") as metadata_outfile:
                    json.dump(metadata, metadata_outfile)
                    print("seasonal information updated")
        else:
            print("seasonal information up to date")
        return metadata


def run_crawler():
    players_count = sum([len(names) for names in [open(PLAYERS_ID_PATH+"/"+f).read().split() for f in os.listdir(PLAYERS_ID_PATH)]])
    print(players_count, "accounts found for data crawling...")

    progress = 0
    for filename in os.listdir(PLAYERS_ID_PATH):
        banned_players = open(PLAYERS_ID_PATH+"/"+filename).read().split()
        print("crawling data for ", len(banned_players),"accounts from file", filename)
        for i in range(0,len(banned_players),6):
            print("\nprogress:", progress+i,"/", players_count, "current:", filename)
            urlstring = ""
            for name in banned_players[i:min(i+6, len(banned_players))]:
                urlstring += name+","
            urlstring = PLAYER_API_REQUEST_ENDPOINT+urlstring[:-1]
            print(urlstring)
            while(requestAndProcessPlayerDataFromAPI(urlstring, headers=header)==429):
                time.sleep(6)
        progress += len(banned_players)

metadata = retrieveUpdatedMetadata()
run_crawler()
