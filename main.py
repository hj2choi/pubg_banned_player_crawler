from scripts.ConfigManager import ConfigManager
from scripts.DataCrawler import DataCrawler

config = ConfigManager("config.json", "metadata.json",
                        players_id_path = "./player_ban_list/",
                        players_data_path = "./banned_players_data/",
                        overwrite_files = False,
                        sample_player_ids_from_match = 0,
                        downloadTelemetry = 3)

crawler = DataCrawler(config)
crawler.run()

print("main()")
