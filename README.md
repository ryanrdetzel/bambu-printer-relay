## BambuLabs 3d Printer Mqtt Relay
This script will connect to all the prints in the config and subscribe to their mqtt updates. Each update is merged to make a complete payload and save in the raw_payload table. If you query this table it gives you the most complete view of the latest status of the printer. In addition, all updates are stored in the raw_history table for debugging and analytics. This can be trimmed by changing the trigger at the top of the script

## Config
Put your printer(s) settings in the config.local.json file. Copy the format from the config.json file.

