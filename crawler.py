import requests
import json
import csv
import time
import os

API_KEY = ""
with open("config.json") as config_file:
    API_KEY = json.load(config_file)["API_KEY"]

PLAYER_API_REQUEST_ENDPOINT = "https://api.pubg.com/shards/kakao/players?filter[playerNames]="
MATCH_API_REQUEST_ENDPOINT = "https://api.pubg.com/shards/steam/matches/"
BANNED_PLAYERS_DIRECTORY = "player_ban_list"

header = {
  "Authorization": API_KEY,
  "Accept": "application/vnd.api+json"
}

# retrieve all relavent match statistics for specified player
def requestMatchStats(match_id, player_id, headers, match_url = "https://api.pubg.com/shards/steam/matches/"):
    match_response = requests.get(match_url+match_id, headers=headers)
    print("fetched match data",match_id,"with response status:",match_response.status_code)

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

        return {"responseStatus": match_response,
                "matchId": match_id,
                "matchAttributes": match_attributes,
                "playerId": player_id,
                "playerStats": in_match_stats,
                "rank": rank,
                "won": won,
                "rosterIdList": roster_id_list,
                "telemetryDataURL": telemetry_URL}
    return {"response_status":match_response}

def writeAllMatchStatsToCsv(filename, player_id, match_list):
    if (len(match_list) ==0):
        return
    with open(filename, mode='w') as csv_file:
        # fetch all matches played in recent 14 days

        fieldnames = ["matchId", "createdAt", "gameMode", "mapName", "isCustomMatch", "duration", "rank", "won",
                        "DBNOs", "assists", "boosts", "damageDealt", "deathType", "headshotKills", "heals",
                        "killPlace", "killPoints", "killPointsDelta", "killStreaks", "kills", "lastKillPoints", "lastWinPoints",
                        "longestKill", "mostDamage", "rankPoints", "revives", "rideDistance", "roadKills", "swimDistance", "teamKills",
                        "timeSurvived", "vehicleDestroys", "walkDistance", "weaponsAcquired", "winPlace", "winPoints", "winPointsDelta",
                        "teammates", "telemetryDataURL"]

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for m in match_list:
            match_data = requestMatchStats(m["id"], player_id, headers=header, match_url=MATCH_API_REQUEST_ENDPOINT)

            roster_str = ""
            for teammate in match_data["rosterIdList"]:
                roster_str+=teammate["name"]+"&"
            roster_str = roster_str[:-1]

            match_data = {**match_data, **match_data["matchAttributes"], **match_data["playerStats"]}
            match_data["teammates"] = roster_str
            for unused_key in ["titleId", "shardId", "playerId", "stats",
                                "rosterIdList", "won", "tags", "matchAttributes","seasonState",
                                "name", "responseStatus", "playerStats"]:
                match_data.pop(unused_key)

            writer.writerow(match_data)

def requestAndProcessPlayerDataFromAPI(url, headers):
    response = requests.get(url, headers=headers)
    print("response status:",response.status_code)
    if response.ok:
        players_dict = json.loads(response.content) # up to 6 players
        # get list of matches of each player
        for player_data in players_dict["data"]:
            print("retrieving",len(player_data["relationships"]["matches"]["data"]),"matches of", player_data["attributes"]["name"])
            writeAllMatchStatsToCsv("processed_data/"+player_data["attributes"]["name"]+".csv",
                                    player_data["id"],
                                    player_data["relationships"]["matches"]["data"])
    return response.status_code

def run_crawler():
    for filename in os.listdir(BANNED_PLAYERS_DIRECTORY):
        banned_players = open(BANNED_PLAYERS_DIRECTORY+"/"+filename).read().split()
        for i in range(0,len(banned_players),6):
            urlstring = ""
            for name in banned_players[i:min(i+6, len(banned_players))]:
                urlstring += name+","
            urlstring = PLAYER_API_REQUEST_ENDPOINT+urlstring[:-1]
            print(urlstring)
            while(requestAndProcessPlayerDataFromAPI(urlstring, headers=header)==429):
                time.sleep(6)

run_crawler()
