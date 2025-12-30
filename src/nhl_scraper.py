import requests
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict
import os

# --- CONFIG ---
TEAMS_TO_WATCH = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", 
    "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", 
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "UTA", "VAN", 
    "VGK", "WSH", "WPG"
]
REFRESH_RATE_MINUTES = 5          

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "nhl_leaders_live.csv") 

def get_team_stats(team_abbr: str) -> List[Dict]:
    """
    Fetches real-time roster stats for a given team abbreviation.
    """
    # Using the new NHL web API endpoint (2024 version)
    url = f"https://api-web.nhle.com/v1/club-stats/{team_abbr}/now"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Stop execution if API is down
        data = response.json()
        
        processed_players = []
        for player in data.get('skaters', []):
            processed_players.append({
                'Team': team_abbr,
                'Player': f"{player['firstName']['default']} {player['lastName']['default']}",
                'Position': player.get('positionCode', 'N/A'),
                # Safety check: gamesPlayed can be 0 for call-ups, defaulting to 1 avoids ZeroDivisionError later
                'GamesPlayed': player.get('gamesPlayed', 1), 
                'Goals': player.get('goals', 0),
                'Assists': player.get('assists', 0),
                'Points': player.get('points', 0),
                'Shots': player.get('shots', 0),
                'PlusMinus': player.get('plusMinus', 0)
            })
        return processed_players
        
    except requests.exceptions.RequestException as e:
        print(f"Network error with {team_abbr}: {e}")
        return []

def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds calculated metrics (PPG, Shooting %) for deeper analysis.
    """
    # Metric 1: Points Per Game (Efficiency)
    df['Pts_Per_Game'] = (df['Points'] / df['GamesPlayed']).round(2)
    
    # Metric 2: Shooting Percentage
    # We only calculate this if players have at least 1 shot to keep data clean
    df['Shooting_Pct'] = df.apply(
        lambda x: round((x['Goals'] / x['Shots']) * 100, 1) if x['Shots'] > 0 else 0.0, axis=1
    )
    
    df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def main():
    print(f" NHL Scraper Live - Tracking: {TEAMS_TO_WATCH}")
    print("   (Ctrl + C to stop)")

    while True:
        all_data = []
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{timestamp}] Ping NHL API...")

        # 1. Fetch data for all teams
        for team in TEAMS_TO_WATCH:
            team_data = get_team_stats(team)
            if team_data:
                all_data.extend(team_data)
                print(f"   -> {team}: Got {len(team_data)} skaters")

        if all_data:
            # 2. Process and Feature Engineering
            df = pd.DataFrame(all_data)
            df_enriched = enrich_data(df)
            
            # Sort by Points to highlight top performers instantly
            df_final = df_enriched.sort_values(by='Points', ascending=False)

            try:
                df_final.to_csv(OUTPUT_FILE, index=False)
                
                leader = df_final.iloc[0]
                print(f"Data Saved. Top Tracked Player: {leader['Player']} ({leader['Team']}) - {leader['Points']} pts")
                
            except PermissionError:
                print("ERROR: Close the CSV file! Cannot write while open.")
        
        else:
            print("No data received (API might be down or empty response).")

        time.sleep(REFRESH_RATE_MINUTES * 60)

if __name__ == "__main__":
    main()