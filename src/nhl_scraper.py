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
    url = f"https://api-web.nhle.com/v1/club-stats/{team_abbr}/now"
    
    try:
        # Standard header to avoid simple bot detection
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        
        # Specific check for Rate Limiting (429)
        if response.status_code == 429:
            print(f"Rate limit hit for {team_abbr}. Waiting 5 seconds...")
            time.sleep(5)
            return []

        response.raise_for_status() 
        data = response.json()
        
        processed_players = []
        for player in data.get('skaters', []):
            processed_players.append({
                'Team': team_abbr,
                'Player': f"{player['firstName']['default']} {player['lastName']['default']}",
                'Position': player.get('positionCode', 'N/A'),
                # We use 0 here because the vectorization handles the math safely
                'GamesPlayed': player.get('gamesPlayed', 0), 
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
    Adds calculated metrics using vectorized Pandas operations for efficiency.
    """
    # Vectorized Points Per Game: fillna(0) handles players with 0 GamesPlayed
    df['Pts_Per_Game'] = (df['Points'] / df['GamesPlayed']).fillna(0).round(2)
    
    # Vectorized Shooting Percentage: (Goals / Shots) * 100
    df['Shooting_Pct'] = (df['Goals'] / df['Shots'] * 100).fillna(0).round(1)
    
    df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def main():
    print(f" NHL Scraper Live - Tracking: {len(TEAMS_TO_WATCH)} teams")
    print("   (Ctrl + C to stop)")

    # Ensure the data directory exists before trying to save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    while True:
        all_data = []
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{timestamp}] Ping NHL API...")

        for team in TEAMS_TO_WATCH:
            team_data = get_team_stats(team)
            if team_data:
                all_data.extend(team_data)
                print(f"   -> {team}: Got {len(team_data)} skaters")

        if all_data:
            df = pd.DataFrame(all_data)
            df_enriched = enrich_data(df)
            
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